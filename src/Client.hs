module Client
(
listFilesReq,
writeFileReq,
readFileReq
) where

import Control.Distributed.Process
import qualified Data.ByteString.Lazy.Char8 as B
import System.FilePath (takeFileName)

import Messages


listFilesReq :: ProcessId -> Process [FileName]
listFilesReq pid = do
  (sendPort,receivePort) <- newChan
  send pid (ListFiles sendPort) --Ask the Namenode for the fsimage
  receiveChan receivePort --Wait to receive the fsimage

writeFileReq :: ProcessId -> FilePath -> Process ()
writeFileReq pid fpath = do
  fdata <- liftIO $ B.readFile fpath --Read file into memory (lazily)
  let fname = B.pack $ takeFileName fpath --get the filename

  (sendPort,receivePort) <- newChan
  send pid (Write fname sendPort) --Send a write request to the namenode

  (bid,sp) <- receiveChan receivePort --We receive the blockid that has been created for that file
                                      --and we also receive the SendPort of the DataNode
  sendChan sp (CDNWrite bid fdata) --Send the filedata to the DataNode

readFileReq :: ProcessId -> FileName -> Process ()
readFileReq pid fname = do
  (sendPort, receivePort) <- newChan
  send pid (Read fname sendPort) --Send a Read request to the namenode

  mfile <- receiveChan receivePort --Receive the file from the network
  case mfile of
    Nothing -> liftIO $ putStrLn "Could not find file on the servers."
    Just file -> liftIO $ B.writeFile ("./data/" ++ (B.unpack fname)) file
