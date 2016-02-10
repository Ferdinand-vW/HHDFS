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
import Network
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
  msg <- B.hGetLine h
  let FilePaths xs = fromByteString msg
  case xs of
    Left e -> error $ show e
    Right fps -> do
      hClose h
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

  putStrLn "got here"
  putStrLn $ show resp
  putStrLn $ show (fromByteString resp :: ProxyToClient)
  putStrLn $ show blockSize
  let WriteAddress res = fromByteString resp

      -- The Datanode sends us back the port to contact the datanode.
      writeBlock fprod (port,bid) = do
        putStrLn $ show bid
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True
        --B.hPutStrLn handle (toByteString $ CDNWrite bid)
        (cons) <- Streams.handleToOutputStream handle
        Streams.write (Just $ toByteString $ CDNWrite bid) cons
        --putStrLn "sending bid"
        fblockprod <- Streams.takeBytes (fromIntegral blockSize) fprod

        Streams.connect fblockprod cons
        --P.runEffect $ fprod P.>-> PB.take (fromIntegral blockSize) P.>-> fcons
        --putStrLn "done sending"
        --Just msg <- Streams.read prod 
        --let WriteComplete = fromByteString msg
        --putStrLn "done"
        return ()
        --hClose handle
  case res of
    Left e -> putStrLn $ show e
    Right addrs -> do
      Streams.withFileAsInput localFile (\fprod -> mapM_ (writeBlock fprod) addrs)
      putStrLn "done writing"

-- Request to read a file. Accepts the remote filepath
readFileReq :: Host -> Handle -> FilePath -> FilePath -> IO Bool
readFileReq host h localPath remoteFile = do

  --fhandle <- IO.openFile localPath IO.WriteMode
  --fcons <- Streams.handleToOutputStream fhandle
  -- Send a read message on the handle
  let rf = toByteString $ Read remoteFile
  putStrLn "got here3"
  B.hPutStrLn h rf
  putStrLn "got here2"
  resp <- B.hGetContents h
  putStrLn "got here"
  putStrLn $ show (fromByteString resp :: ProxyToClient)
  let ReadAddress mexists = fromByteString resp

      -- We start reading blocks from the handle
      readBlock fcons (port,bid) = do
        putStrLn "Setup connection to datanode"
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        putStrLn "Connected to datanode"
        (prod,cons) <- Streams.handleToStreams handle
        putStrLn "Try to send read message"
        Streams.write (Just $ toByteString $ CDNRead bid) cons
        putStrLn "Send read message"
        Streams.supply prod fcons
        putStrLn "Got data"
  case mexists of
    Left e -> do
      putStrLn (show e)
      return False
    Right addrs -> do
      hClose h
      -- fold readblocks to build up the file from all the received blocks
      putStrLn "gets here"
      Streams.withFileAsOutput localPath (\fcons -> mapM_ (readBlock fcons) addrs)
      putStrLn "should be done now"
      return True

chunksOf :: Int -> L.ByteString -> [L.ByteString]
chunksOf n s = case L.splitAt (fromIntegral n) s of
  (a,b) | L.null a  -> []
        | otherwise -> a : chunksOf n b
