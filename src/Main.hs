module Main where

import System.Environment
import Client (client)
--import DataNode (dataNode)
import NameNode (nameNode)
import NodeInitialization

main = do
    prog <- getArgs
    case prog of
        ["client"] -> initNode client
--        ["datanode"] -> initNode dataNode
        ["namenode"] -> initNameNode nameNode
