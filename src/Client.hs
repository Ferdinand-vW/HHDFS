module Client
(
client
)
where

import System.IO
import Control.Concurrent (threadDelay)
import System.FilePath (takeFileName)
import System.Directory (createDirectoryIfMissing)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary(encode,decode)
import Control.Monad (forever,void)
import Network

import ClientAPI
import Messages

-- Example client
client :: Host -> Port -> IO ()
client host port = do
  input <- getLine --parse some input

  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h NoBuffering
  hSetBinaryMode h True
  void $ case words input of
          ["show"] -> do
            putStrLn "Contents:"
            fsimage <- listFilesReq h
            showFSImage fsimage
          ["write",localFile,remotePath] -> writeFileReq host h localFile remotePath >> putStrLn "Done writing" --Write a file onto the network
          ["read",path] -> do
              readFileReq host h (localPath path) path >> putStrLn "Done reading"--Retrieve the file
              --writeToDisk path mfdata --Write file to diskk
          ["quit"] -> putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
          _ -> (putStrLn "Input was not a valid command.")
  hClose h
  client host port


--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FilePath] -> IO ()
showFSImage fsimage = mapM_ (\x -> putStrLn $ "  " ++ x) fsimage

writeToDisk :: FilePath -> Maybe FileData -> IO ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> putStrLn "Could not find file on network"
  Just fdata -> do
      createDirectoryIfMissing False "./local"
      L.writeFile ("./local/" ++ takeFileName fpath) fdata

localPath :: String -> String
localPath fpath = "./local/" ++ takeFileName fpath
