{-# LANGUAGE DeriveDataTypeable, DeriveGeneric #-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Char8 as B

type BlockId = Int
type Position = (ProcessId, BlockId)
type FileData = B.ByteString

data HandShake = HandShake
  { dataNodeId :: ProcessId
  }
  deriving (Typeable, Generic)

data ClientReq = ListFiles (SendPort [FilePath])
               | Read FilePath (SendPort ClientRes)
               | Write FilePath (SendPort ClientRes)
               | Shutdown
  deriving (Typeable, Generic)

type ClientRes = Either ClientError Position

data ClientError = InvalidPathError
                 | FileNotFound
  deriving (Typeable, Generic, Show)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
            | CDNRep BlockId [ProcessId]
  deriving (Typeable, Generic)
  
data BlockReport = BlockReport ProcessId [BlockId]
  deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientError
instance Binary ClientReq
instance Binary CDNReq
instance Binary BlockReport
