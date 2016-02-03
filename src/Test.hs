module Test where

import System.IO
import System.Directory
import Control.Distributed.Process
import Network
import System.Random (mkStdGen, randoms)
import Control.Monad (zipWithM_)
import System.FilePath (takeFileName)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Criterion.Main

import Messages (FileData, FileName, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)


smallFiles :: [String]
smallFiles = ["smallfile1","smallfile2","smallfile3","smallfile4","smallfile5",
              "smallfile6","smallfile7","smallfile8","smallfile9","smallfile10"]

testDir :: String
testDir = "./test_files/"

testFileCount :: Int
testFileCount = 1

testClient :: String -> String -> IO ()
testClient host port = do
  putStrLn "Starting tests..."

  defaultMain [
    bgroup "test1" [ bench "1" $ nfIO $ writeManySmallFiles host port
                   , bench "2" $ nfIO $ writeAndReadManySmallFiles host port
                   ]
              ]

getPath fname = testDir ++ fname
fileOut fname = fname ++ ".out"

writeAndReadManySmallFiles :: Host -> Port -> IO ()
writeAndReadManySmallFiles h p = testWriteAndRead h p smallFiles

writeManySmallFiles :: Host -> Port -> IO ()
writeManySmallFiles h p = testManyWrites h p smallFiles

testManyWrites :: Host -> Port -> [FileName] -> IO ()
testManyWrites host port fnames = do
  mapM_ (testWrite host port) fnames

testWrite :: Host -> Port -> FileName -> IO ()
testWrite host port fName = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

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

  let filename = "file" ++ show fName ++ ".out"
  putStrLn $ "read " ++ fName
  file <- readFileReq host h (fileOut fName)
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
