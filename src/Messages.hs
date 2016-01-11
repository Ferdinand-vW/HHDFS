{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Lazy as B

type BlockId = Int
type Position = (ProcessId, BlockId)
type FileData = B.ByteString
type FileName = B.ByteString

data HandShake = HandShake
  { sendPort :: SendPort CDNReq
  }
  deriving (Typeable, Generic)

data ClientReq = ListFiles (SendPort [FileName])
               | Read FileName (SendPort (Maybe FileData))
               | Write FileName (SendPort (BlockId, SendPort CDNReq))
  deriving (Typeable, Generic)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
  deriving (Typeable, Generic)


instance Binary HandShake
instance Binary ClientReq
instance Binary CDNReq
