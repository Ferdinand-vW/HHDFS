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

import Messages (FileData)
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
    bgroup "test1" [ bench "1" $ nfIO $ computation host port
                   , bench "2" $ nfIO $ computation host port
                   ]
              ]
 
getFilename n = testDir ++ "/file" ++ show n
fileIn n  = getFilename n ++ ".in"
fileOut n = getFilename n ++ ".out"         

computation :: String -> String -> IO ()
computation host port = do
  let fileNums = [0 .. testFileCount - 1]
      testFile n = do
                h <- connectTo host (PortNumber $ fromIntegral $ read port)
                hSetBuffering h LineBuffering

                putStrLn $ "write " ++ (fileIn n) ++ " " ++ (fileOut n)
                writeFileReq host h (fileIn n) (fileOut n)

                hClose h
  mapM_ testFile fileNums

  let readBack n = do
        h <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering h LineBuffering

        let filename = "file" ++ show n ++ ".out"
        putStrLn $ "read " ++ fileOut n
        file <- readFileReq host h (fileOut n)
        writeToDisk filename file

        hClose h
  mapM_ readBack fileNums

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
