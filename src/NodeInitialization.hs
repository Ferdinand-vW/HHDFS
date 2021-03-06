module NodeInitialization
(
setupClient,
setupDataNode,
setupNameNode
)
where

import           Control.Exception
import           Control.Monad (forever)
import           Control.Concurrent(threadDelay,forkIO)
import           Control.Distributed.Process
import           Control.Distributed.Process.Node
import           Network.Transport hiding (connect,send,receive)
import           Network.Transport.TCP
import           Network hiding(sClose)
import           Network.Socket
import           Network.BSD
import qualified Data.ByteString.Char8 as B
import           Data.Binary (encode,decode)
import           System.IO
import           System.Directory (createDirectoryIfMissing)

import Messages
import NameNodeProxy
import DataNodeProxy

type Addr = String

setupClient :: (Host -> Port -> IO()) -> Host -> Port -> IO ()
setupClient p host port = do
  putStrLn "Starting Client..."
  createDirectoryIfMissing False "./local"
  --First we try to connect to the proxy server
  p host port
  return ()

setupDataNode :: (Port -> ProcessId -> Process()) -> Host -> Port -> Addr -> IO ()
setupDataNode p host port addr = do
  putStrLn "Starting DataNode..."
  Right t <- createTransport host port defaultTCPParameters --setup transport layer for the node
  node <- newLocalNode t initRemoteTable --create a new localnode using the transport layer
  let nnAddr = EndPointAddress $ B.pack addr
      nodeid = NodeId nnAddr --Create a NodeId for the NameNode
  runProcess node $ do --We start a process on the node
    whereisRemoteAsync nodeid "NameNodePid" --See if we can find the NameNode and if so get his ProcessId
    WhereIsReply _ mpid <- expect :: Process WhereIsReply
    case mpid of
        Nothing -> liftIO $ putStrLn $ "Could not connect to NameNode with address " ++ addr
        Just npid -> do
          pid <- getSelfPid
          spawnLocal $ waitForConnections host port pid
          p port npid --Continue with the given Process and pass it the NameNode ProcessId

setupNameNode :: Process () -> Host -> Port -> IO ()
setupNameNode p host port = do
  putStrLn "Starting NameNode..."
  et <- createTransport host port defaultTCPParameters
  case et of
    Left f -> putStrLn $ show f
    Right t -> do
      node <- newLocalNode t initRemoteTable
      runProcess node $ do
          pid <- getSelfPid --Dynamically register the NameNode's ProcessId
          spawnLocal $ listenForClients host port pid

          register "NameNodePid" pid
          p

listenForClients :: Host -> Port -> ProcessId -> Process ()
listenForClients host port pid = do
  socket <- liftIO $ listenOn' (PortNumber $ fromIntegral $ 1 + read port)
  namenodeproxy socket pid

waitForConnections :: Host -> Port -> ProcessId -> Process ()
waitForConnections host port pid = do
  socket <- liftIO $ listenOn' (PortNumber $ fromIntegral $ 1 + read port)
  datanodeproxy socket pid

--Pretty much a copy-paste from the Network package. We only changed the number of
--queued connections
listenOn' (PortNumber port) = do
    proto <- getProtocolNumber "tcp"
    bracketOnError
        (socket AF_INET Stream proto)
        (sClose)
        (\sock -> do
            setSocketOption sock ReuseAddr 1
            bindSocket sock (SockAddrInet port iNADDR_ANY)
            listen sock 2048
            return sock
        )
