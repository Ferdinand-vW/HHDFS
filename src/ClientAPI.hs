module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq,
shutdownReq
) where

import Control.Distributed.Process
import qualified Data.ByteString.Lazy.Char8 as B
import Messages

listFilesReq :: ProcessId -> Process [FilePath]
listFilesReq pid = do
  (sendPort,receivePort) <- newChan
  send pid (ListFiles sendPort) --Ask the Namenode for the fsimage
  receiveChan receivePort --Wait to receive the fsimage

writeFileReq :: ProcessId -> FilePath -> FilePath -> Process ()
writeFileReq pid localFile remotePath = do
  fdata <- liftIO $ B.readFile localFile --Read file into memory (lazily)

  (sendPort,receivePort) <- newChan
  send pid (Write remotePath sendPort) --Send a write request to the namenode

  res <- receiveChan receivePort --We receive the blockid that has been created for that file
                                      --and we also receive the SendPort of the DataNode

  case res of
    Right (pid,bid) -> send pid (CDNWrite bid fdata) --Send the filedata to the DataNode
    Left e -> do
      say $ show e
      return ()

readFileReq :: ProcessId -> FilePath -> Process (Maybe FileData)
readFileReq pid fpath = do
  (sendPort, receivePort) <- newChan
  send pid (Read fpath sendPort) --Send a Read request to the namenode

  mexists <- receiveChan receivePort --Receive the file location

  case mexists of
    Left e -> do
      say $ show e
      return Nothing
    Right (pid,bid) -> do
      (sp,rp) <- newChan
      send pid (CDNRead bid sp) --Send a read request to the datanode
      fdata <- receiveChan rp --Receive the file
      return $ Just fdata

shutdownReq :: ProcessId -> Process ()
shutdownReq pid = send pid Shutdown  -- Send a shutdown request to the name node
                                      -- This will close every node in the network
