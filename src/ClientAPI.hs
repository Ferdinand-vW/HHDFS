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
import Data.Binary(decode)
import Network
import System.IO

import Messages

listFilesReq :: Handle -> IO [FilePath]
listFilesReq h = do
  let lf = toByteString ListFiles
  L.hPutStrLn h lf
  msg <- L.hGetContents h
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
  fdata <- L.readFile localFile

  -- calculate the number of blocks required
  flength <- IO.withFile localFile IO.ReadMode IO.hFileSize
  let blockCount = 1 + fromIntegral (flength `div` blockSize)

  -- send the request to the datanode and wait for a response
  L.hPutStrLn h $ toByteString $ Write remotePath blockCount
  resp <- L.hGetContents h
  let WriteAddress res = fromByteString resp

      -- The Datanode sends us back the port to contact the datanode.
      writeBlock (port,bid) fblock = do
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True

        L.hPut handle (toByteString $ CDNWrite bid fblock)
        hClose handle
  case res of
    Left e -> putStrLn $ show e
    Right addrs -> zipWithM_ writeBlock addrs (chunksOf (fromIntegral blockSize) fdata) >> putStrLn "done writing"

-- Request to read a file. Accepts the remote filepath
readFileReq :: Host -> Handle -> FilePath -> IO (Maybe FileData)
readFileReq host h fpath = do

  -- Send a read message on the handle
  let rf = toByteString $ Read fpath
  L.hPutStrLn h rf

  resp <- L.hGetContents h
  let ReadAddress mexists = fromByteString resp

      -- We start reading blocks from the handle
      readBlock bs (port,bid) = do
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True
        open <- hIsOpen handle
        L.hPutStrLn handle (toByteString $ CDNRead bid)
        fdata <- L.hGetContents handle
        let FileBlock fd = fromByteString fdata
        return $ L.append bs fd
  case mexists of
    Left e -> putStrLn (show e) >> return Nothing
    Right addrs -> do
      hClose h
      -- fold readblocks to build up the file from all the received blocks
      Just <$> foldM readBlock L.empty addrs

chunksOf :: Int -> L.ByteString -> [L.ByteString]
chunksOf n s = case L.splitAt (fromIntegral n) s of
  (a,b) | L.null a  -> []
        | otherwise -> a : chunksOf n b
