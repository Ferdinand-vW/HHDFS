module NodeInitialization
(
setupProxy,
setupClient,
setupNode,
setupNameNode
)
where

import Control.Concurrent(threadDelay,forkIO)
import Control.Concurrent.MVar
import Control.Distributed.Process hiding(handleMessage)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Internal.Types
import Network.Transport hiding (connect,send,receive)
import Network.Transport.TCP
import qualified Data.ByteString.Char8 as B
import Data.Binary (encode,decode)
import System.IO
import Network
import Control.Monad (forever)

import Messages
import NameNodeProxy
import DataNodeProxy

type Addr = String

setupProxy :: (ProcessId -> Socket -> IO ()) -> Host -> Port -> Addr -> IO ()
setupProxy p host port addr = do
    withSocketsDo $ do
      socket <- listenOn (PortNumber (fromIntegral $ read port))
      (h,_,_) <- accept socket
      B.hPutStrLn h (B.pack "test")
    {-Right t <- createTransport host port defaultTCPParameters --setup transport layer for the node
    node <- newLocalNode t initRemoteTable --create a new localnode using the transport layer
    let nnAddr = EndPointAddress $ B.pack addr
        nodeid = NodeId nnAddr --Create a NodeId for the NameNode
    pidT <- newEmptyMVar
    runProcess node $ do --We start a process on the node
      whereisRemoteAsync nodeid "NameNodePid" --See if we can find the NameNode and if so get his ProcessId
      WhereIsReply _ mpid <- expect :: Process WhereIsReply
      case mpid of
          Nothing -> liftIO $ putStrLn $ "Could not connect to NameNode with address " ++ addr
          Just pid -> say "connected to namenode" >> liftIO (putMVar pidT pid)
    pid <- takeMVar pidT --Wait for a response from the NameNode
    let pidlocal = processLocalId pid
        nodeid   = processNodeId
        nodeEpAd = B.pack "84.105.186.154:44444:0"
        pid' = ProcessId (NodeId $ EndPointAddress nodeEpAd) pidlocal

    putStrLn $ show pid'
    closeTransport t --Close the transport, because it is no longer needed

  --Now we start listening for Client connections and send them the ProcessId of the NameNode
    withSocketsDo $ do
      addrinfos <- getAddrInfo
                   (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                   Nothing (Just port)
      let proxyAddr = head addrinfos
      sock <- socket (addrFamily proxyAddr) Stream defaultProtocol
      bindSocket sock (addrAddress proxyAddr)
      listen sock 5
      p pid sock-}
    

setupClient :: (Handle -> IO()) -> Host -> Port -> Host -> Port -> IO ()
setupClient p host port phost pport = do
  --First we try to connect to the proxy server
  h <- connectTo phost (PortNumber $ fromIntegral $ read pport)
  hSetBuffering h LineBuffering
  p h
    {-addrinfos <- getAddrInfo Nothing (Just phost) (Just pport)
    let proxyAddr = head addrinfos
    sock <- socket (addrFamily proxyAddr) Stream defaultProtocol
    setSocketOption sock KeepAlive 1
    connect sock (addrAddress proxyAddr)
    msg <- recv sock 1024
    let Response pid = decode $ L.fromStrict msg-}
  return ()
  --runProcess node $ p pid



{-receivePId :: EndPoint -> IO ProcessId
receivePId endpoint = do
  event <- receive endpoint
  putStrLn "received message"
  case event of
    Received _ payload -> do
      let Connected pid = decode $ L.fromStrict $ head $ payload
      return pid
    _ -> receivePId endpoint-}

setupNode :: (Port -> ProcessId -> Process()) -> Host -> Port -> Addr -> IO ()
setupNode p host port addr = do
    Right t <- createTransport host port defaultTCPParameters --setup transport layer for the node
    node <- newLocalNode t initRemoteTable --create a new localnode using the transport layer
    let nnAddr = EndPointAddress $ B.pack addr
        nodeid = NodeId nnAddr --Create a NodeId for the NameNode
    runProcess node $ do --We start a process on the node
      whereisRemoteAsync nodeid "NameNodePid" --See if we can find the NameNode and if so get his ProcessId
      WhereIsReply _ mpid <- expect :: Process WhereIsReply
      case mpid of
          Nothing -> liftIO $ putStrLn $ "Could not connect to NameNode with address " ++ addr
          Just npid -> say "connected to namenode" >> do
            pid <- getSelfPid
            spawnLocal $ waitForConnections host port pid
            p port npid --Continue with the given Process and pass it the NameNode ProcessId
        
setupNameNode :: Process () -> Host -> Port -> IO ()
setupNameNode p host port = do
    et <- createTransport host port defaultTCPParameters
    case et of
      Left f -> putStrLn $ show f
      Right t -> do
        node <- newLocalNode t initRemoteTable
        runProcess node $ do
            pid <- getSelfPid --Dynamically register the NameNode's ProcessId
            spawnLocal $ listenForClients host port pid
            
            say "Started process"
            register "NameNodePid" pid
            p

listenForClients :: Host -> Port -> ProcessId -> Process ()
listenForClients host port pid = do
  socket <- liftIO $ listenOn (PortNumber $ fromIntegral $ 1 + read port)
  namenodeproxy socket pid

waitForConnections :: Host -> Port -> ProcessId -> Process ()
waitForConnections host port pid = do
  say "Start listening"
  socket <- liftIO $ listenOn (PortNumber $ fromIntegral $ 1 + read port)
  datanodeproxy socket pid