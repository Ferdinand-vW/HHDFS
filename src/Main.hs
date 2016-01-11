module Main where

import System.Environment (getArgs)

import NodeInitialization (setupNameNode, setupNode)
import DataNode (dataNode)
import Client (client)

main :: IO ()
main = do
  (mode : args) <- getArgs

  --address should be of the form: host:port:0
  case mode of
    "namenode" ->
      case args of
        [port] ->      setupNameNode undefined "127.0.0.1" port
        [host,port] -> setupNameNode undefined host port
    "datanode" ->
      case args of
        [port,addr] ->      setupNode dataNode "127.0.0.1" port addr
        [host,port,addr] -> setupNode dataNode host port addr
    "client" ->
      case args of
        [port,addr] ->      setupNode client "127.0.0.1" port addr
        [host,port,addr] -> setupNode client host port addr


