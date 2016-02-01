module Test where

import System.Directory
import Control.Distributed.Process
import Control.Monad (when)
import System.Random (mkStdGen, randoms)

--client :: ProcessId -> Process ()
--client pid = do
--  input <- liftIO getLine --parse some input
--  case words input of
--    ["show"] -> do
--      fsimage <- listFilesReq pid --get the filenames from the namenode
--      showFSImage fsimage
--      client pid
--    ["write",localFile,remotePath] -> writeFileReq pid localFile remotePath >> client pid --Write a file onto the network
--    ["read",path] -> do
--        mfdata <- readFileReq pid path --Retrieve the file
--        writeToDisk path mfdata --Write file to disk
--        client pid --Read a file from the network
--    ["quit"] -> liftIO $ putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
--    ["shutdown"] -> liftIO (putStrLn "Shutting down Network...") >> shutdownReq pid >> terminate
--    _ -> liftIO (putStrLn "Input was not a valid command.") >> client pid

testDir :: String
testDir = "./test_files"

testClient :: ProcessId -> Process ()
testClient pid = do
  liftIO $ createTestDir
  let ints = randoms (mkStdGen 0) :: [Int]
  let (file, ints') = generateFile ints
  liftIO $ putStrLn "Starting tests..."
  liftIO $ writeFile (testDir ++ "/file1.txt") file

createTestDir :: IO ()
createTestDir = do
  exists <- doesDirectoryExist testDir
  case exists of
    True  -> removeDirectoryRecursive testDir
    False -> return ()
  createDirectory testDir

generateFile :: Show a => [a] -> (String, [a])
generateFile as = go 100000 as ""
  where go 0 as acc = (acc, as)
        go n as acc = go (n-1) as' (line ++ acc)
          where (line, as') = generateLine as

generateLine :: Show a => [a] -> (String, [a])
generateLine as = (text, ys)
  where (xs,ys) = splitAt 10 as
        text = concatMap ((' ':) . show) xs