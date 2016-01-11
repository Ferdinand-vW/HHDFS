module ClientAPI
(
listFilesReq,
writeFileReq,
readFileReq
) where

import Control.Distributed.Process
import qualified Data.ByteString.Lazy.Char8 as B
import Messages

listFilesReq :: ProcessId -> Process [FilePath]
listFilesReq pid = do
  (sendPort,receivePort) <- newChan
  send pid (ListFiles sendPort) --Ask the Namenode for the fsimage
  receiveChan receivePort --Wait to receive the fsimage

writeFileReq :: ProcessId -> FilePath -> Process ()
writeFileReq pid fpath = do
  fdata <- liftIO $ B.readFile fpath --Read file into memory (lazily)

  (sendPort,receivePort) <- newChan
  send pid (Write fpath sendPort) --Send a write request to the namenode

  (pid,bid) <- receiveChan receivePort --We receive the blockid that has been created for that file
                                      --and we also receive the SendPort of the DataNode
  send pid (CDNWrite bid fdata) --Send the filedata to the DataNode

readFileReq :: ProcessId -> FilePath -> Process (Maybe FileData)
readFileReq pid fpath = do
  (sendPort, receivePort) <- newChan
  send pid (Read fpath sendPort) --Send a Read request to the namenode

  mexists <- receiveChan receivePort --Receive the file location
  
  case mexists of
    Nothing -> return Nothing
    Just (pid,bid) -> do
        (sp,rp) <- newChan
        send pid (CDNRead bid sp) --Send a read request to the datanode
        fdata <- receiveChan rp --Receive the file
        return $ Just fdata
