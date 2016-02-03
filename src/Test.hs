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

import Messages (FileData, FileName, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)


smallFiles :: [String]
smallFiles = ["smallfile1","smallfile2","smallfile3","smallfile4","smallfile5",
              "smallfile6","smallfile7","smallfile8","smallfile9","smallfile10"]

testDir :: String
testDir = "./test_files/"

testFileCount :: Int
testFileCount = 1

files1 = ["test1.txt","test2.txt","test3.txt"]
files2 = ["test4.txt","test5.txt","test6.txt"]

testClient :: String -> String -> IO ()
testClient host port = do
  putStrLn "Starting tests..."

  before <- getTime Realtime
  res <- mapConcurrently (asyncTestWriteAndRead host port) [files1,files2]
  after <- getTime Realtime
  putStrLn $ show $ diffTimeSpec after before
  _ <- getLine
  return ()

getPath fname = testDir ++ fname
fileOut fname = fname ++ ".out"

writeAndReadManySmallFiles :: Host -> Port -> IO ()
writeAndReadManySmallFiles h p = testWriteAndRead h p smallFiles

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

writeManySmallFiles :: Host -> Port -> IO ()
writeManySmallFiles h p = testManyWrites h p smallFiles

testManyWrites :: Host -> Port -> [FileName] -> IO ()
testManyWrites host port fnames = do
  mapM_ (testWrite host port) fnames

testWrite :: Host -> Port -> FileName -> IO ()
testWrite host port fName = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  putStrLn $ "writing " ++ fName ++ " -> " ++ fName
  let
    fIn = getPath fName
    fOut = fileOut fName
  putStrLn $ "writing " ++ fIn ++ " -> " ++ fOut
  writeFileReq host h fIn fOut

  hClose h

testRead :: Host -> Port -> String -> IO ()
testRead host port fName = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  let filename = "file" ++ fName ++ ".out"
  putStrLn $ "read " ++ fName
  file <- readFileReq host h fName
  writeToDisk filename file

  hClose h

testWriteAndRead :: Host -> Port -> [FileName] -> IO ()
testWriteAndRead host port fNames = do
  mapM_ (testWrite host port) fNames
  mapM_ (testRead host port) (map fileOut fNames)


writeToDisk :: FilePath -> Maybe FileData -> IO ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> putStrLn "Could not find file on network"
  Just fdata -> do
      createDirectoryIfMissing False "./local"
      B.writeFile ("./local/" ++ takeFileName fpath) (L.toStrict fdata) --Had to add L.toStrict
