{-# LANGUAGE ScopedTypeVariables #-}

module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq
) where

import           Control.Distributed.Process
import           Network.Socket
import           Control.Monad(unless)
import qualified Data.ByteString.Char8 as B
import qualified System.IO.Streams as Streams
import           System.IO.Streams (InputStream, OutputStream)
import           System.IO

import Messages

listFilesReq :: Handle -> IO (Either ClientError [FilePath])
listFilesReq h = do
  let lf = toByteString ListFiles
  B.hPutStrLn h lf
  msg <- B.hGetContents h
  let FilePaths xs = fromByteString msg
  return xs

-- Request to write a file. Accepts a local and a remote filepath.
writeFileReq :: Host -> Handle -> FilePath -> FilePath -> IO ()
writeFileReq host h localFile remotePath = do
  -- read data from the local file
    -- calculate the number of blocks required
  flength <- withFile localFile ReadMode hFileSize
  let blockCount = ceiling $ fromIntegral flength / fromIntegral blockSize
  putStrLn $ show blockCount
  unless (blockCount == 0) $ do
    -- send the request to the datanode and wait for a response
    let wf = Write remotePath blockCount
    B.hPutStrLn h $ toByteString wf
    resp <- B.hGetContents h
    putStrLn $ show resp
    let WriteAddress res = fromByteString resp
        writeBlock fprod (port,bid) = withSocketsDo $ do
          sock <- openConnection host port --Open a connection to the datanode
          (prod,cons) <- Streams.socketToStreams sock --Convert the socket into an input and output stream

          Streams.write (Just $ toByteString $ CDNWrite bid) cons --Send the write message to the datanode
          Just ok <- Streams.read prod --Wait for confirmation from the datanode to start writing
          fblockprod <- Streams.takeExactly (fromIntegral blockSize) fprod --Take an exact number of bytes from the file stream
          Streams.supply fblockprod cons --Send these bytes to the datanode

          closeConnection sock --We can now close the connection

        finalWriteBlock fprod (port,bid) = withSocketsDo $ do
          sock <- openConnection host port
          (prod,cons) <- Streams.socketToStreams sock

          Streams.write (Just $ toByteString $ CDNWrite bid) cons
          Just rsp <- Streams.read prod
          fblockprod <- Streams.takeBytes (fromIntegral blockSize) fprod --Take remaining bytes
          Streams.connect fblockprod cons --Signal EOF

          closeConnection sock
    case res of
      Left e -> print e
      Right addrs ->
        Streams.withFileAsInput localFile (\fprod -> do --Open a file and convert it into an output stream
          mapM_ (writeBlock fprod) (init addrs) --Write blocks to datanodes
          finalWriteBlock fprod (last addrs)) --For the final block we have to close the file output stream

-- Request to read a file. Accepts the remote filepath
readFileReq :: Host -> Handle -> FilePath -> FilePath -> IO Bool
readFileReq host h localPath remoteFile = do

  let rf = toByteString $ Read remoteFile
  B.hPutStrLn h rf --Ask namenode for block locations
  resp <- B.hGetContents h --get a response back

  let ReadAddress mexists = fromByteString resp
      -- We start reading blocks from the handle
      readBlock fcons (port,bid) = withSocketsDo $ do
        sock <- openConnection host port
        (prod,cons) <- Streams.socketToStreams sock

        Streams.write (Just $ toByteString $ CDNRead bid) cons --Send a read message to the datanode
        fblockprod <- Streams.takeExactly (fromIntegral blockSize) prod --Take the bytes from the datanode
        Streams.supply fblockprod fcons --Pass the bytes into the file stream

        closeConnection sock

      finalReadBlock fcons (port,bid) = withSocketsDo $ do
        sock <- openConnection host port
        (prod,cons) <- Streams.socketToStreams sock

        Streams.write (Just $ toByteString $ CDNRead bid) cons
        fblockprod <- Streams.takeBytes (fromIntegral blockSize) prod
        Streams.connect fblockprod fcons --Signal EOF

        closeConnection sock
  case mexists of
    Left e -> do
      print e
      return False
    Right addrs -> do
      hClose h
      -- fold readblocks to build up the file from all the received blocks
      Streams.withFileAsOutput localPath (\fcons -> do
        mapM_ (readBlock fcons) (init addrs)
        finalReadBlock fcons (last addrs))
      return True

--Standard way of connecting to a Server using sockets
openConnection :: Host -> Port -> IO Socket
openConnection host port = do
  addrInfo <- getAddrInfo Nothing (Just host) (Just port)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)
  return sock

closeConnection :: Socket -> IO ()
closeConnection sock = sClose sock
