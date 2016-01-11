module NodeInitialization
(
initNode,
initNameNode
)
where

import Control.Distributed.Process
import Control.Distributed.Process.Node
import Network.Transport
import Network.Transport.TCP
import Data.ByteString.Char8 (pack)

type Host = String
type Port = String
type Addr = String

--Use this function to startup the NameNode
initNameNode :: Process () -> IO ()
initNameNode p = do
    args <- getLine
    case words args of
        [port] ->      setupNameNode p "127.0.0.1" port
        [host,port] -> setupNameNode p host port

--Use this function to startup any other node (DataNode/Client)
initNode :: (ProcessId -> Process ()) -> IO ()
initNode p = do
    args <- getLine
    --address should be of the form: host:port:0
    case words args of
        [port,addr] -> setupNode p "127.0.0.1" port addr
        [host,port,addr] -> setupNode p host port addr

setupNode :: (ProcessId -> Process()) -> Host -> Port -> Addr -> IO ()
setupNode p host port addr = do
    Right t <- createTransport host port defaultTCPParameters --setup transport layer for the node
    node <- newLocalNode t initRemoteTable --create a new localnode using the transport layer
    let nnAddr = EndPointAddress $ pack addr
        nodeid = NodeId nnAddr --Create a NodeId for the NameNode
    runProcess node $ do --We start a process on the node
      whereisRemoteAsync nodeid "NameNodePid" --See if we can find the NameNode and if so get his ProcessId
      WhereIsReply _ mpid <- expect :: Process WhereIsReply
      case mpid of
          Nothing -> liftIO $ putStrLn $ "Could not connect to NameNode with address " ++ addr
          Just pid -> p pid --Continue with the given Process and pass it the NameNode ProcessId
        
setupNameNode :: Process () -> Host -> Port -> IO ()
setupNameNode p host port = do
    Right t <- createTransport host port defaultTCPParameters
    node <- newLocalNode t initRemoteTable
    runProcess node $ do
        pid <- getSelfPid --Dynamically register the NameNode's ProcessId
        register "NameNodePid" pid
        p
