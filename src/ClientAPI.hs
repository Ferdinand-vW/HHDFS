{-# LANGUAGE ScopedTypeVariables #-}

module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq
) where

import Control.Distributed.Process

import Data.Functor ((<$>))
import Control.Monad (foldM, zipWithM_,unless)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary(decode,encode)
import Network hiding (sClose)
import Network.Socket
import System.IO
import qualified Pipes.ByteString as PB
import qualified Pipes as P
import System.IO.Streams (InputStream, OutputStream)
import qualified System.IO.Streams as Streams

import Messages

listFilesReq :: Handle -> IO [FilePath]
listFilesReq h = do
  let lf = toByteString ListFiles
  B.hPutStrLn h lf
  msg <- B.hGetContents h
  let FilePaths xs = fromByteString msg
  case xs of
    Left e -> error $ show e
    Right fps -> do
      return fps

-- Request to write a file. Accepts a local and a remote filepath.
writeFileReq :: Host -> Handle -> FilePath -> FilePath -> IO ()
writeFileReq host h localFile remotePath = do
  -- read data from the local file
    -- calculate the number of blocks required
  flength <- IO.withFile localFile IO.ReadMode IO.hFileSize
  let blockCount = 1 + fromIntegral (flength `div` blockSize)

  -- send the request to the datanode and wait for a response
  B.hPutStrLn h $ toByteString $ Write remotePath blockCount
  resp <- B.hGetContents h

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
    Left e -> putStrLn $ show e
    Right addrs -> do
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
      readBlock fcons (port,bid) = do
        sock <- openConnection host port
        (prod,cons) <- Streams.socketToStreams sock

        Streams.write (Just $ toByteString $ CDNRead bid) cons --Send a read message to the datanode
        fblockprod <- Streams.takeExactly (fromIntegral blockSize) prod --Take the bytes from the datanode
        Streams.supply fblockprod fcons --Pass the bytes into the file stream

        closeConnection sock

      finalReadBlock fcons (port,bid) = do
        sock <- openConnection host port
        (prod,cons) <- Streams.socketToStreams sock

        Streams.write (Just $ toByteString $ CDNRead bid) cons
        fblockprod <- Streams.takeBytes (fromIntegral blockSize) prod
        Streams.connect fblockprod fcons --Signal EOF

        closeConnection sock
  case mexists of
    Left e -> do
      putStrLn (show e)
      return False
    Right addrs -> do
      hClose h
      -- fold readblocks to build up the file from all the received blocks
      Streams.withFileAsOutput localPath (\fcons -> do
        mapM_ (readBlock fcons) (init addrs)
        finalReadBlock fcons (last addrs))
      return True

chunksOf :: Int -> L.ByteString -> [L.ByteString]
chunksOf n s = case L.splitAt (fromIntegral n) s of
  (a,b) | L.null a  -> []
        | otherwise -> a : chunksOf n b

openConnection :: Host -> Port -> IO Socket
openConnection host port = do
  addrInfo <- getAddrInfo Nothing (Just host) (Just port)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)
  return sock

closeConnection :: Socket -> IO ()
closeConnection sock = do
  sClose sock