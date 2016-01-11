module Main where

import System.Environment
import Client (client)
import DataNode (dataNode)
import NameNode (nameNode)
import NodeInitialization

main = do
    prog <- getArgs
    case prog of
        ["client",host,port,addr] -> setupNode client host port addr
        ["datanode",host,port,addr] -> setupNode dataNode host port addr
        ["namenode",host,port] -> setupNameNode nameNode host port
