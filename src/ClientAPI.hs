module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq,
shutdownReq
) where

import Control.Distributed.Process

import Data.Functor ((<$>))
import Control.Monad (foldM, zipWithM_)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import qualified System.IO as IO
import Data.Binary(decode)
import Network
import System.IO

import Messages

listFilesReq :: Handle -> IO [FilePath]
listFilesReq h = do
  let lf = toByteString ListFiles
  L.hPutStrLn h lf
  msg <- L.hGetContents h
  let FilePaths xs = fromByteString msg
  case xs of
    Left e -> error $ show e
    Right fps -> return fps

writeFileReq :: Host -> Handle -> FilePath -> FilePath -> IO ()
writeFileReq host h localFile remotePath = do
  fdata <- L.readFile localFile

  flength <- IO.withFile localFile IO.ReadMode IO.hFileSize
  let blockCount = 1 + fromIntegral (flength `div` blockSize)
  L.hPutStrLn h $ toByteString $ Write remotePath blockCount
  resp <- L.hGetContents h
  let WriteAddress res = fromByteString resp
      writeBlock (port,bid) fblock = do
        putStrLn "Write a block"
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True

        putStrLn "Was able to connect"
        L.hPut handle (toByteString $ CDNWrite bid fblock)
        putStrLn "Send a message"
        hClose handle
  putStrLn $ "Received write address: " ++ show res
  case res of
    Left e -> putStrLn $ show e
    Right addrs -> zipWithM_ writeBlock addrs (chunksOf (fromIntegral blockSize) fdata) >> putStrLn "done writing"

readFileReq :: Host -> Handle -> FilePath -> IO (Maybe FileData)
readFileReq host h fpath = do
  let rf = toByteString $ Read fpath
  L.hPutStrLn h rf

  resp <- L.hGetContents h
  let ReadAddress mexists = fromByteString resp
      readBlock bs (port,bid) = do
        putStrLn "Try to connect to datanode"
        handle <- connectTo host (PortNumber $ fromIntegral $ read port)
        hSetBuffering handle NoBuffering
        hSetBinaryMode handle True
        open <- hIsOpen handle
        putStrLn "Connected to datanode"
        L.hPutStrLn handle (toByteString $ CDNRead bid)
        putStrLn "Send read command"
        fdata <- L.hGetContents handle
        putStrLn "Received data"
        let FileBlock fd = fromByteString fdata
        return $ L.append bs fd
  putStrLn "received read addresses"
  case mexists of
    Left e -> putStrLn (show e) >> return Nothing
    Right addrs -> Just <$> foldM readBlock L.empty addrs

chunksOf :: Int -> L.ByteString -> [L.ByteString]
chunksOf n s = case L.splitAt (fromIntegral n) s of
  (a,b) | L.null a  -> []
        | otherwise -> a : chunksOf n b

shutdownReq :: Handle -> IO ()
shutdownReq h = undefined--B.hPutStrLn h (toByteString Shutdown)  -- Send a shutdown request to the name node
                                      -- This will close every node in the network

{-listFilesReq :: ProcessId -> Process [FilePath]
listFilesReq pid = do
  (sendPort,receivePort) <- newChan
  --send pid (ListFiles sendPort) --Ask the Namenode for the fsimage
  res <- receiveChan receivePort --Wait to receive the fsimage

  case res of
    Right fp -> return fp
    Left e -> do
      say $ show e
      return []

writeFileReq :: ProcessId -> FilePath -> FilePath -> Process ()
writeFileReq pid localFile remotePath = do
  fdata <- liftIO $ B.readFile localFile --Read file into memory (lazily)

  flength <- liftIO $ IO.withFile localFile IO.ReadMode IO.hFileSize
  let blockCount = 1 + fromIntegral (flength `div` blockSize)

  (sendPort,receivePort) <- newChan

  send pid (Write remotePath blockCount sendPort) --Send a write request to the namenode

  res <- receiveChan receivePort --We receive the blockid that has been created for that file
                                      --and we also receive the SendPort of the DataNode

  let writeBlock (pid,bid) fblock = send pid (CDNWrite bid fblock)

  case res of
    Right ps -> zipWithM_ writeBlock ps (chunksOf (fromIntegral blockSize) fdata) --Send the filedata to the DataNode
    Left e -> say $ show e

readFileReq :: ProcessId -> FilePath -> Process (Maybe FileData)
readFileReq pid fpath = do
  (sendPort, receivePort) <- newChan
  send pid (Read fpath sendPort) --Send a Read request to the namenode

  mexists <- receiveChan receivePort --Receive the file location

  let readBlock :: FileData -> RemotePosition -> Process FileData
      readBlock bs (pid,bid) = do
        (sp,rp) <- newChan
        send pid (CDNRead bid sp) --Send a read request to the datanode
        fdata <- receiveChan rp --Receive the file
        return $ B.append bs fdata -- TODO appending here is probably slow

  case mexists of
    Left e -> do
      say $ show e
      return Nothing
    Right ps -> Just <$> foldM readBlock B.empty ps
-}
