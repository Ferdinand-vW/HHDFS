import Control.Distributed.Process
import Control.Concurrent (threadDelay)
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as B
import System.FilePath (takeFileName)
import Messages
import NodeInitialization


main = initNode client

--Main loop
client :: ProcessId -> Process ()
client pid = do
    input <- liftIO $ getLine --parse some input
    case words input of
        ["show"] -> do --Client wants to see all the files stored on the server
            fsimage <- fsImageReq pid --get the filenames from the namenode
            showFSImage fsimage --Visually show the filenames
            client pid
        ["write",path] -> writeFileReq pid path >> client pid --Write a file onto the network
        ["read",name] -> readFileReq pid (B.pack name) >> client pid --Read a file from the network
        ["quit"] -> liftIO $ putStrLn "Closing program..." >> threadDelay 2000000 --Print a message and after 2 seconds quit
        _ -> liftIO (putStrLn "Input was not a valid command.") >> client pid
        
        
fsImageReq :: ProcessId -> Process [FileName]
fsImageReq pid = do
    (sendPort,receivePort) <- newChan
    send pid (Show sendPort) --Ask the Namenode for the fsimage
    receiveChan receivePort --Wait to receive the fsimage

--Simply prints out all the filenames prefixed with 2 spaces
showFSImage :: [FileName] -> Process ()
showFSImage fsimage = do
    mapM_ (\x -> liftIO . putStrLn $ "  " ++ (B.unpack x)) fsimage
    
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
    
