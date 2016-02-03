module Test where

import System.IO
import System.Directory
import GHC.Conc(forkIO)
import Control.Distributed.Process
import Network
import System.Random (mkStdGen, randoms)
import Control.Monad (zipWithM_)
import System.FilePath (takeFileName)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import System.Clock
import Control.Concurrent.Async

import Messages (FileData, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)

testDir :: String
testDir = "./test_files"

testFileCount :: Int
testFileCount = 3

files1 = ["test1.txt","test2.txt","test3.txt"]
files2 = ["test4.txt","test5.txt","test6.txt"]

testClient :: String -> String -> IO ()
testClient host port = do
  createTestDir
  let files = zip generateFiles [0..]



  putStrLn "Generating files..."
  mapM_ (\(f,n) -> writeFile (fileIn n) f) (take testFileCount files)

  putStrLn "Starting tests..."

  before <- getTime Realtime
  res <- mapConcurrently (asyncTestWriteAndRead host port) [files1,files2]
  after <- getTime Realtime
  putStrLn $ show $ diffTimeSpec after before
  _ <- getLine
  return ()

getFilename n = testDir ++ "/file" ++ show n
fileIn n  = getFilename n ++ ".in"
fileOut n = getFilename n ++ ".out"

asyncTestWriteAndRead :: Host -> Port -> [String] -> IO ()
asyncTestWriteAndRead host port fps = do
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  testWriteAndRead host port fps
  return ()

testManyWrites :: Host -> Port -> [String] -> IO ()
testManyWrites host port fps = do
  let fileNums = [0 .. testFileCount - 1]
  mapM_ (testWrite host port) fps

testWrite :: Host -> Port -> String -> IO ()
testWrite host port fp = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  putStrLn $ "writing " ++ fp ++ " -> " ++ fp
  writeFileReq host h fp fp

  hClose h

testRead :: Host -> Port -> String -> IO ()
testRead host port fp = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  let filename = "file" ++ fp ++ ".out"
  putStrLn $ "read " ++ fp
  file <- readFileReq host h fp
  writeToDisk filename file

  hClose h

testWriteAndRead :: String -> String -> [String] -> IO ()
testWriteAndRead host port fps = do
  let fileNums = [0 .. testFileCount - 1]
  mapM_ (testWrite host port) fps

  mapM_ (testRead host port) fps

createTestDir :: IO ()
createTestDir = do
  exists <- doesDirectoryExist testDir
  case exists of
    True  -> removeDirectoryRecursive testDir
    False -> return ()
  createDirectory testDir

generateFiles :: [String]
generateFiles = go ints
  where ints = randoms (mkStdGen 0) :: [Int]
        go as = let (file, ys) = generateFile as
                in  file : go ys

generateFile :: Show a => [a] -> (String, [a])
generateFile as = go (100000 :: Int) as ""
  where go 0 as acc = (acc, as)
        go n as acc = go (n-1) as' (line ++ "\n"++ acc)
          where (line, as') = generateLine as

generateLine :: Show a => [a] -> (String, [a])
generateLine as = (text, ys)
  where (xs,ys) = splitAt 10 as
        text = concatMap ((' ':) . show) xs


writeToDisk :: FilePath -> Maybe FileData -> IO ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> putStrLn "Could not find file on network"
  Just fdata -> do
      createDirectoryIfMissing False "./local"
      B.writeFile ("./local/" ++ takeFileName fpath) (L.toStrict fdata) --Had to add L.toStrict
