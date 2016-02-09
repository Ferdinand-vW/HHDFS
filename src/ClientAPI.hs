{-# LANGUAGE ScopedTypeVariables #-}

module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq
) where

import Control.Distributed.Process

import Data.Functor ((<$>))
import Control.Monad (foldM, zipWithM_)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary(decode,encode)
import Network
import System.IO
import qualified Pipes.ByteString as PB
import qualified Pipes as P

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

  fhandle <- IO.openFile localFile IO.ReadMode
  let fprod = PB.fromHandle fhandle

  let WriteAddress res = fromByteString resp

      -- The Datanode sends us back the port to contact the datanode.
      writeBlock (port,bid) = do
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True
        B.hPutStrLn handle (toByteString $ CDNWrite bid)
        let fcons = PB.toHandle handle

        P.runEffect $ fprod P.>-> PB.take (fromIntegral blockSize) P.>-> fcons
        --L.hPut handle (toByteString $ CDNWrite bid fblock)
        --hClose handle
  case res of
    Left e -> do
      putStrLn $ show e
      IO.hClose fhandle
    Right addrs -> do
      mapM_ writeBlock addrs
      putStrLn "done writing"
      IO.hClose fhandle

-- Request to read a file. Accepts the remote filepath
readFileReq :: Host -> Handle -> FilePath -> FilePath -> IO Bool
readFileReq host h localPath remoteFile = do

  fhandle <- IO.openFile localPath IO.WriteMode
  let fcons = PB.toHandle fhandle
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
      readBlock (port,bid) = do
        putStrLn "Setup connection to datanode"
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        putStrLn "Connected to datanode"
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True
        open <- hIsOpen handle
        putStrLn "Try to send read message"
        B.hPutStrLn handle (toByteString $ CDNRead bid)
        let fprod = PB.fromHandle handle
        P.runEffect $ fprod P.>-> fcons

  case mexists of
    Left e -> do
      putStrLn (show e)
      IO.hClose fhandle
      return False
    Right addrs -> do
      hClose h
      -- fold readblocks to build up the file from all the received blocks
      putStrLn "gets here"
      mapM_ readBlock addrs
      IO.hClose fhandle
      putStrLn "should be done now"
      return True

chunksOf :: Int -> L.ByteString -> [L.ByteString]
chunksOf n s = case L.splitAt (fromIntegral n) s of
  (a,b) | L.null a  -> []
        | otherwise -> a : chunksOf n b
