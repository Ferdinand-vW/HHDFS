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
import qualified System.IO as IO

import Messages

listFilesReq :: ProcessId -> Process [FilePath]
listFilesReq pid = do
  (sendPort,receivePort) <- newChan
  send pid (ListFiles sendPort) --Ask the Namenode for the fsimage
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

shutdownReq :: ProcessId -> Process ()
shutdownReq pid = send pid Shutdown  -- Send a shutdown request to the name node
                                      -- This will close every node in the network


chunksOf :: Int -> B.ByteString -> [B.ByteString]
chunksOf n s = case B.splitAt (fromIntegral n) s of
  (a,b) | B.null a  -> []
        | otherwise -> a : chunksOf n b
