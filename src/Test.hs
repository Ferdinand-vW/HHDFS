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
import Control.Distributed.Process.Async

import Messages (FileData, FileName, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)


smallFiles :: [String]
smallFiles = ["smallfile1","smallfile2","smallfile3","smallfile4","smallfile5",
              "smallfile6","smallfile7","smallfile8","smallfile9","smallfile10"]

bigFiles :: [String]
bigFiles = ["bigfile1", "bigfile2", "bigfile3", "bigfile4"]

testDir :: String
testDir = "./test_files/"

testClient :: ProcessId -> Process ()
testClient pid = do
  liftIO $ putStrLn "Starting tests..."

  before <- liftIO $ getTime Realtime
  writeManySmallFiles pid
  after <- liftIO $ getTime Realtime
  liftIO $ print $ diffTimeSpec after before
  _ <- liftIO $ getLine

  before <- liftIO $ getTime Realtime
  writeAndReadManySmallFiles pid
  after <- liftIO $ getTime Realtime
  liftIO $ print $ diffTimeSpec after before
  _ <- liftIO $ getLine

  before <- liftIO $ getTime Realtime
  writeAndReadManyBigFiles pid
  after <- liftIO $ getTime Realtime
  liftIO $ print $ diffTimeSpec after before
  return ()

getPath fname = testDir ++ fname
fileOut fname = fname ++ ".out"

writeAndReadManySmallFiles :: ProcessId -> Process ()
writeAndReadManySmallFiles pid = testWriteAndRead pid smallFiles

writeAndReadManyBigFiles :: ProcessId -> Process ()
writeAndReadManyBigFiles pid = testWriteAndRead pid bigFiles

writeManySmallFiles :: ProcessId -> Process ()
writeManySmallFiles pid = testManyWrites pid smallFiles

testManyWrites :: ProcessId -> [FileName] -> Process ()
testManyWrites pid fnames = do
                  mapConcurrently (testWrite pid) fnames

testWriteAndRead :: ProcessId -> [FileName] -> Process ()
testWriteAndRead pid fNames = do
  mapConcurrently (testWrite pid) fNames
  mapConcurrently (testRead pid) (map fileOut fNames)

testWrite :: ProcessId -> FileName -> Process ()
testWrite pid fName = do
  liftIO $ putStrLn $ "writing " ++ fName ++ " -> " ++ fName
  let
    fIn = getPath fName
    fOut = fileOut fName
  liftIO $ putStrLn $ "writing " ++ fIn ++ " -> " ++ fOut
  writeFileReq pid fIn fOut


testRead :: ProcessId -> String -> Process ()
testRead pid fName = do
  let filename = "file" ++ fName ++ ".out"
  liftIO $ putStrLn $ "read " ++ fName
  file <- readFileReq pid fName
  liftIO $ putStrLn "was able to read"
  liftIO $ writeToDisk filename file

writeToDisk :: FilePath -> Maybe FileData -> IO ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> putStrLn "Could not find file on network"
  Just fdata -> do
      createDirectoryIfMissing False "./local"
      B.writeFile ("./local/" ++ takeFileName fpath) fdata --Had to add L.toStrict

mapConcurrently :: (a -> Process ()) -> [a] -> Process ()
mapConcurrently f xs = do
        asyncs <- mapM (async . task . f) xs
        mapM_ wait asyncs
