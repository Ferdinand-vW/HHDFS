{-# LANGUAGE DeriveDataTypeable, DeriveGeneric#-}

module Messages where

import Data.Typeable
import GHC.Generics
import Data.Binary (Binary)
import Control.Distributed.Process
import qualified Data.ByteString.Lazy as B

-- Block size in bytes. For now, very small for testing purposes
blockSize :: Integer
blockSize = 32

type BlockId = Int
type BlockCount = Int
type Position = (ProcessId, BlockId)
type FileData = B.ByteString


data HandShake = HandShake
  { dataNodeId :: ProcessId
  }
  deriving (Typeable, Generic)

data ClientReq = ListFiles (SendPort [FilePath])
               | Read FilePath (SendPort (ClientRes [Position]))
               | Write FilePath BlockCount (SendPort (ClientRes [Position]))
               | Shutdown
  deriving (Typeable, Generic)

type ClientRes a = Either ClientError a

data ClientError = InvalidPathError
                 | FileNotFound
  deriving (Typeable, Generic, Show)

data CDNReq = CDNRead BlockId (SendPort FileData)
            | CDNWrite BlockId FileData
            | CDNDelete BlockId
  deriving (Typeable, Generic)

instance Binary HandShake
instance Binary ClientError
instance Binary ClientReq
instance Binary CDNReq
