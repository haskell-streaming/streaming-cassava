{-# LANGUAGE DeriveFunctor, FlexibleContexts, MultiParamTypeClasses,
             OverloadedStrings #-}

{- |
   Module      : Streaming.Cassava
   Description : Cassava support for the streaming library
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Streaming.Cassava where

import qualified Data.ByteString                    as DB
import qualified Data.ByteString.Lazy               as DBL
import           Data.ByteString.Streaming          (ByteString)
import qualified Data.ByteString.Streaming          as B
import qualified Data.ByteString.Streaming.Internal as B
import           Streaming
import qualified Streaming.Prelude                  as S

import           Data.Csv             (DecodeOptions, EncodeOptions, FromRecord,
                                       ToRecord, defaultDecodeOptions,
                                       defaultEncodeOptions)
import           Data.Csv.Incremental (HasHeader(..), Parser(..))
import qualified Data.Csv.Incremental as CI

import Control.Exception         (Exception(..))
import Control.Monad.Error.Class (MonadError, throwError)
import Control.Monad.Trans.Class (lift)
import Data.Bifunctor            (first)
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
decodeWithErrors opts = loop . CI.decodeWith opts
  where
    feed f str = (uncurry loop . maybe (f mempty, str) (first f))
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

getValues :: (MonadError e m) => Stream (Of (Either e a)) m r -> Stream (Of a) m r
getValues = S.mapM (either throwError return)

newtype CsvParseException = CsvParseException String
  deriving (Eq, Show, Typeable)

instance IsString CsvParseException where
  fromString = CsvParseException

instance Exception CsvParseException where
  displayException (CsvParseException e) = "Error parsing csv: " ++ e

--------------------------------------------------------------------------------

-- | Encode a stream of values with the default options.
--
--   Optionally prefix the stream with headers.
encode :: (ToRecord a, Monad m) => Maybe [DB.ByteString]
          -> Stream (Of a) m r -> ByteString m r
encode = encodeWith defaultEncodeOptions

-- | Encode a stream of values with the provided options.
--
--   Optionally prefix the stream with headers.
encodeWith :: (ToRecord a, Monad m) => EncodeOptions -> Maybe [DB.ByteString]
              -> Stream (Of a) m r -> ByteString m r
encodeWith opts mhdr = B.fromChunks
                       . S.concat
                       . addHeaders
                       . S.map enc
  where
    addHeaders = maybe id (S.cons . enc) mhdr

    enc :: (ToRecord v) => v -> [DB.ByteString]
    enc = DBL.toChunks . CI.encodeWith opts . CI.encodeRecord
