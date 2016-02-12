module Client
(
client
)
where

import System.IO
import Control.Concurrent (threadDelay)
import System.FilePath (takeFileName)
import Control.Monad (void)
import Network

import ClientAPI
import Messages

-- Example client
client :: Host -> Port -> IO ()
client host port = do
  putStr "> "
  hFlush stdout
  input <- getLine --parse some input

  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h NoBuffering
  hSetBinaryMode h True
  cont <- case words input of
            ["help"] -> do
              putStrLn "show : Lists all files on the network"
              putStrLn "write local remote : Enter a local file path and a file name to write a file to the network"
              putStrLn "read remote : Enter a file on the network to read it from the network"
              putStrLn "quit : Closes the client application"
              return True
            ["show"] -> do
              eFsImage <- listFilesReq h --Get the filesystem image from the network
              handleFsImage eFsImage --Print the results to stdout
              return True
            ["write",localFile,remotePath] -> do
              writeFileReq host h localFile remotePath
              putStrLn "Finished writing" --Write a file onto the network
              return True
            ["read",path] -> do
              exists <- readFileReq host h (localPath path) path --Retrieve the file
              fileExists exists
              return True
            ["quit"] -> do
              putStrLn "Closing program..."
              threadDelay 2000000 --Print a message and after 2 seconds quit
              return False
            _ -> do
              (putStrLn "Input was not a valid command.")
              return True
  hClose h

  if cont --Determine whether we continue or not
    then client host port
    else return ()

--Either print out the error or print out all filenames
handleFsImage :: Either ClientError [FilePath] -> IO ()
handleFsImage (Left err) = putStrLn $ show err
handleFsImage (Right fp) = showFsImage fp

--Simply prints out all the filenames prefixed with 2 spaces
showFsImage :: [FilePath] -> IO ()
showFsImage fsimage = do
  putStrLn "Contents:"
  mapM_ (\x -> putStrLn $ "  " ++ x) fsimage

fileExists :: Bool -> IO ()
fileExists False = putStrLn "Could not find file on network"
fileExists True  = putStrLn "Successfully read file."

localPath :: String -> String
localPath fpath = "./local/" ++ takeFileName fpath
