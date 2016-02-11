module Test where



import           Control.Monad (zipWithM_)
import           Control.Concurrent.Async
import           Control.Distributed.Process
import           GHC.Conc (forkIO)
import           Network
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import           System.Clock
import           System.IO
import           System.Directory
import           System.FilePath (takeFileName)
import           System.Random (mkStdGen, randoms)

import Messages (FileData, FileName, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq)


smallFiles :: [String]
smallFiles = ["mediumfile1","mediumfile2","mediumfile3","mediumfile4","mediumfile5"]

bigFiles :: [String]
bigFiles = [ "bigfile1", "bigfile2", "bigfile3", "bigfile4", "bigfile5", "bigfile6",
             "bigfile7", "bigfile8", "bigfile9", "bigfile10", "bigfile11", "bigfile12"]

testDir :: String
testDir = "./test_files/"

testClient :: String -> String -> IO ()
testClient host port = do
  createDirectoryIfMissing False "./local"

  putStrLn "Starting tests..."

  before <- getTime Realtime
  writeManySmallFiles host port
  after <- getTime Realtime
  print $ diffTimeSpec after before
  _ <- getLine

  before <- getTime Realtime
  writeAndReadManySmallFiles host port
  after <- getTime Realtime
  print $ diffTimeSpec after before
  _ <- getLine

  before <- getTime Realtime
  writeAndReadManyBigFiles host port
  after <- getTime Realtime
  print $ diffTimeSpec after before
  return ()

getPath fname = testDir ++ fname
fileOut fname = fname ++ ".out"

writeAndReadManySmallFiles :: Host -> Port -> IO [()]
writeAndReadManySmallFiles h p = testWriteAndRead h p smallFiles

writeAndReadManyBigFiles :: Host -> Port -> IO [()]
writeAndReadManyBigFiles h p = testWriteAndRead h p bigFiles

writeManySmallFiles :: Host -> Port -> IO [()]
writeManySmallFiles h p = testManyWrites h p smallFiles

testManyWrites :: Host -> Port -> [FileName] -> IO [()]
testManyWrites host port fnames = mapConcurrently (testWrite host port) fnames

testWriteAndRead :: Host -> Port -> [FileName] -> IO [()]
testWriteAndRead host port fNames = do
  mapConcurrently (testWrite host port) fNames
  mapConcurrently (testRead host port) (map fileOut fNames)

testWrite :: Host -> Port -> FileName -> IO ()
testWrite host port fName = do
  h <- getHandle host port
  putStrLn $ "writing " ++ fName ++ " -> " ++ fName
  let
    fIn = getPath fName
    fOut = fileOut fName
  putStrLn $ "writing " ++ fIn ++ " -> " ++ fOut
  writeFileReq host h fIn fOut

  hClose h

testRead :: Host -> Port -> String -> IO ()
testRead host port fName = do
  h <- getHandle host port

  let filename = "file" ++ fName ++ ".out"
      local = "local/" ++ filename
  putStrLn $ "read " ++ fName
  file <- readFileReq host h local fName
  -- We do not account for the time taken to write a file to disk when running tests (ClientAPI has changed since then)
  hClose h

getHandle :: Host -> Port -> IO Handle
getHandle h p = do
  h <- connectTo h (PortNumber $ fromIntegral $ read p)
  hSetBuffering h NoBuffering
  hSetBinaryMode h True
  return h
