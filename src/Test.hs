module Test where

import System.Directory
import Control.Distributed.Process
import System.Random (mkStdGen, randoms)
import Control.Monad (zipWithM_)
import System.FilePath (takeFileName)
import qualified Data.ByteString.Char8 as B

import Messages (FileData)
import ClientAPI (listFilesReq,writeFileReq,readFileReq, shutdownReq)

testDir :: String
testDir = "./test_files"

testFileCount :: Int
testFileCount = 3

testClient :: ProcessId -> Process ()
testClient pid = do
  liftIO $ createTestDir
  let files = zip generateFiles [0..]

      getFilename n = testDir ++ "/file" ++ show n
      fileIn n  = getFilename n ++ ".in"
      fileOut n = getFilename n ++ ".out"

  liftIO $ putStrLn "Generating files..."
  liftIO $ mapM_ (\(f,n) -> writeFile (fileIn n) f) (take testFileCount files)

  liftIO $ putStrLn "Starting tests..."
  let fileNums = [0 .. testFileCount - 1]
      testFile n = do
        liftIO $ putStrLn $ "write " ++ (fileIn n) ++ " " ++ (fileOut n)
        writeFileReq pid (fileIn n) (fileOut n)
  mapM_ testFile fileNums

  let readBack n = do
        let filename = "file" ++ show n ++ ".out"
        liftIO $ putStrLn $ "read " ++ fileOut n
        file <- readFileReq pid (fileOut n)
        writeToDisk filename file
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


writeToDisk :: FilePath -> Maybe FileData -> Process ()
writeToDisk fpath mfdata = case mfdata of
  Nothing -> liftIO $ putStrLn "Could not find file on network"
  Just fdata -> liftIO $ do
      createDirectoryIfMissing False "./local"
      B.writeFile ("./local/" ++ takeFileName fpath) fdata
