{-# LANGUAGE FlexibleContexts, MultiParamTypeClasses, OverloadedStrings,
             ScopedTypeVariables #-}

{- |
   Module      : Streaming.Cassava
   Description : Cassava support for the streaming library
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Streaming.Cassava
  ( -- * Decoding
    decode
  , decodeWith
  , decodeWithErrors
  , CsvParseException (..)
    -- ** Named decoding
  , decodeByName
  , decodeByNameWith
  , decodeByNameWithErrors
    -- * Encoding
  , encode
  , encodeDefault
  , encodeWith
    -- ** Named encoding
  , encodeByName
  , encodeByNameDefault
  , encodeByNameWith
    -- * Re-exports
  , FromRecord (..)
  , FromNamedRecord (..)
  , ToRecord (..)
  , ToNamedRecord (..)
  , DefaultOrdered (..)
  , HasHeader (..)
  , Header
  , header
  , Name
  , DecodeOptions(..)
  , defaultDecodeOptions
  , EncodeOptions(..)
  , defaultEncodeOptions
  ) where

import qualified Data.ByteString                    as DB
import qualified Data.ByteString.Lazy               as DBL
import           Data.ByteString.Streaming          (ByteString)
import qualified Data.ByteString.Streaming          as B
import qualified Data.ByteString.Streaming.Internal as B
import           Streaming                          (Of, Stream)
import qualified Streaming.Prelude                  as S

import           Data.Csv             (DecodeOptions, DefaultOrdered(..),
                                       EncodeOptions, FromNamedRecord(..),
                                       FromRecord(..), Header, Name,
                                       ToNamedRecord(..), ToRecord(..),
                                       defaultDecodeOptions,
                                       defaultEncodeOptions, encIncludeHeader,
                                       header)
import qualified Data.Csv             as C
import           Data.Csv.Incremental (HasHeader(..), HeaderParser(..),
                                       Parser(..))
import qualified Data.Csv.Incremental as CI

import Control.Exception         (Exception(..))
import Control.Monad.Error.Class (MonadError, throwError)
import Control.Monad.Trans.Class (lift)
import Data.Bifunctor            (first)
import Data.Maybe                (fromMaybe)
import Data.String               (IsString(..))
import Data.Typeable             (Typeable)

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decode :: (MonadError CsvParseException m, FromRecord a)
          => HasHeader -> ByteString m r
          -> Stream (Of a) m r
decode = decodeWith defaultDecodeOptions

-- | Return back a stream of values from the provided CSV, stopping at
--   the first error.
--
--   If you wish to instead ignore errors, consider using
--   'decodeWithErrors' with either 'S.mapMaybe' or @'S.effects'
--   . 'S.partitionEithers'@.
--
--   Unlike 'decodeWithErrors', any remaining input is discarded.
decodeWith :: (MonadError CsvParseException m, FromRecord a)
              => DecodeOptions -> HasHeader
              -> ByteString m r -> Stream (Of a) m r
decodeWith opts hdr bs = getValues (decodeWithErrors opts hdr bs)
                         >>= either (throwError . fst) return

-- | Return back a stream with an attempt at type conversion, and
--   either the previous result or any overall parsing errors with the
--   remainder of the input.
decodeWithErrors :: (Monad m, FromRecord a) => DecodeOptions -> HasHeader
                    -> ByteString m r
                    -> Stream (Of (Either CsvParseException a)) m (Either (CsvParseException, ByteString m r) r)
decodeWithErrors opts = runParser . CI.decodeWith opts

runParser :: (Monad m) => Parser a -> ByteString m r
             -> Stream (Of (Either CsvParseException a)) m (Either (CsvParseException, ByteString m r) r)
runParser = loop
  where
    feed f str = (uncurry (loop . f) . fromMaybe (mempty, str))
                          -- nxt == Nothing, str is just Return
                 =<< lift (B.unconsChunk str)

    loop p str = case p of
                   Fail bs err -> return (Left (CsvParseException err, B.consChunk bs str))
                   Many es get -> withEach es >> feed get str
                   Done es     -> do withEach es
                                     -- This is primarily just to
                                     -- return the @r@ value, but also
                                     -- acts as a check on the parser.
                                     nxt <- lift (B.nextChunk str)
                                     return $ case nxt of
                                                Left r  -> Right r
                                                Right _ -> Left ("Unconsumed input", str)

    withEach = S.each . map (first CsvParseException)

--------------------------------------------------------------------------------

-- | Use 'defaultOptions' for decoding the provided CSV.
decodeByName :: (MonadError CsvParseException m, FromNamedRecord a)
                => ByteString m r -> Stream (Of a) m r
decodeByName = decodeByNameWith defaultDecodeOptions

-- | Return back a stream of values from the provided CSV, stopping at
--   the first error.
--
--   A header is required to determine the order of columns, but then
--   discarded.
--
--   If you wish to instead ignore errors, consider using
--   'decodeByNameWithErrors' with either 'S.mapMaybe' or @'S.effects'
--   . 'S.partitionEithers'@.
--
--   Unlike 'decodeByNameWithErrors', any remaining input is
--   discarded.
decodeByNameWith :: (MonadError CsvParseException m, FromNamedRecord a)
                    => DecodeOptions
                    -> ByteString m r -> Stream (Of a) m r
decodeByNameWith opts bs = getValues (decodeByNameWithErrors opts bs)
                           >>= either (throwError . fst) return

-- | Return back a stream with an attempt at type conversion, but
--   where the order of columns doesn't have to match the order of
--   fields of your actual type.
--
--   This requires\/assumes a header in the CSV stream, which is
--   discarded after parsing.
decodeByNameWithErrors :: (Monad m, FromNamedRecord a) => DecodeOptions
                          -> ByteString m r
                          -> Stream (Of (Either CsvParseException a)) m (Either (CsvParseException, ByteString m r) r)
decodeByNameWithErrors = loopH . CI.decodeByNameWith
  where
    loopH ph str = case ph of
                     FailH bs err -> return (Left (CsvParseException err, B.consChunk bs str))
                     PartialH get -> feedH get str
                     DoneH _  p   -> runParser p str

    feedH f str = (uncurry (loopH . f) . fromMaybe (mempty, str))
                           -- nxt == Nothing, str is just Return
                  =<< lift (B.unconsChunk str)

--------------------------------------------------------------------------------

-- | Encode a stream of values with the default options.
--
--   Optionally prefix the stream with headers (the 'header' function
--   may be useful).
encode :: (ToRecord a, Monad m) => Maybe Header
          -> Stream (Of a) m r -> ByteString m r
encode = encodeWith defaultEncodeOptions

-- | Encode a stream of values with the default options and a derived
--   header prefixed.
encodeDefault :: forall a m r. (ToRecord a, DefaultOrdered a, Monad m)
                 => Stream (Of a) m r -> ByteString m r
encodeDefault = encode (Just (headerOrder (undefined :: a)))

-- | Encode a stream of values with the provided options.
--
--   Optionally prefix the stream with headers (the 'header' function
--   may be useful).
encodeWith :: (ToRecord a, Monad m) => EncodeOptions -> Maybe Header
              -> Stream (Of a) m r -> ByteString m r
encodeWith opts mhdr = B.fromChunks
                       . S.concat
                       . addHeaders
                       . S.map enc
  where
    addHeaders = maybe id (S.cons . enc) mhdr

    enc :: (ToRecord v) => v -> [DB.ByteString]
    enc = DBL.toChunks . CI.encodeWith opts . CI.encodeRecord

--------------------------------------------------------------------------------

-- | Use the default ordering to encode all fields\/columns.
encodeByNameDefault :: forall a m r. (DefaultOrdered a, ToNamedRecord a, Monad m)
                       => Stream (Of a) m r -> ByteString m r
encodeByNameDefault = encodeByName (headerOrder (undefined :: a))

-- | Select the columns that you wish to encode from your data
--   structure using default options (which currently includes
--   printing the header).
encodeByName :: (ToNamedRecord a, Monad m) => Header
                -> Stream (Of a) m r -> ByteString m r
encodeByName = encodeByNameWith defaultEncodeOptions

-- | Select the columns that you wish to encode from your data
--   structure.
--
--   Header printing respects 'encIncludeheader'.
encodeByNameWith :: (ToNamedRecord a, Monad m) => EncodeOptions -> Header
                    -> Stream (Of a) m r -> ByteString m r
encodeByNameWith opts hdr = B.fromChunks
                            . S.concat
                            . addHeaders
                            . S.map enc
  where
    opts' = opts { encIncludeHeader = False }

    addHeaders
      | encIncludeHeader opts = S.cons . DBL.toChunks
                                . CI.encodeWith opts' . CI.encodeRecord $ hdr
      | otherwise             = id

    enc = DBL.toChunks . C.encodeByNameWith opts' hdr . (:[])

--------------------------------------------------------------------------------

getValues :: (MonadError e m) => Stream (Of (Either e a)) m r -> Stream (Of a) m r
getValues = S.mapM (either throwError return)

newtype CsvParseException = CsvParseException String
  deriving (Eq, Show, Typeable)

instance IsString CsvParseException where
  fromString = CsvParseException

instance Exception CsvParseException where
  displayException (CsvParseException e) = "Error parsing csv: " ++ e
