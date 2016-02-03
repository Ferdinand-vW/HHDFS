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

import Messages (FileData, Host, Port)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)

testDir :: String
testDir = "./test_files"

testFileCount :: Int
testFileCount = 3

testClient :: String -> String -> IO ()
testClient host port = do
  createTestDir
  let files = zip generateFiles [0..]



  putStrLn "Generating files..."
  mapM_ (\(f,n) -> writeFile (fileIn n) f) (take testFileCount files)

  putStrLn "Starting tests..."

  defaultMain [
    bgroup "test1" [ bench "1" $ nfIO $ testManyWrites host port
                   -- , bench "2" $ nfIO $ testWriteAndRead host port
                   ]
              ]

getFilename n = testDir ++ "/file" ++ show n
fileIn n  = getFilename n ++ ".in"
fileOut n = getFilename n ++ ".out"

testManyWrites :: Host -> Port -> IO ()
testManyWrites host port = do
  let fileNums = [0 .. testFileCount - 1]
  mapM_ (testWrite host port) fileNums

testWrite :: Host -> Port -> Int -> IO ()
testWrite host port fName = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  let
    fIn = fileIn fName
    fOut = fileOut fName
  putStrLn $ "writing " ++ fIn ++ " -> " ++ fOut
  writeFileReq host h fIn fOut

  hClose h

testRead :: Host -> Port -> Int -> IO ()
testRead host port fName = do
  h <- connectTo host (PortNumber $ fromIntegral $ read port)
  hSetBuffering h LineBuffering

  let filename = "file" ++ show fName ++ ".out"
  putStrLn $ "read " ++ fileOut fName
  file <- readFileReq host h (fileOut fName)
  writeToDisk filename file

  hClose h

testWriteAndRead :: String -> String -> IO ()
testWriteAndRead host port = do
  let fileNums = [0 .. testFileCount - 1]
  mapM_ (testWrite host port) fileNums

  mapM_ (testRead host port) fileNums

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
