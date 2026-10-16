#!/usr/bin/env sh
# Regenerate src/bindings.rs after building this crate once (the configured xmlversion.h lives in
# the build output). Requires bindgen-cli 0.72.1 and libclang.
set -eu
include_dir="${1:?usage: update-bindings.sh <build-out>/include/libxml2}"
cd "$(dirname "$0")"
bindgen src/wrapper.h -o src/bindings.rs --use-core --no-doc-comments \
  --default-enum-style consts --no-prepend-enum-name \
  --allowlist-function 'xmlInitParser|xmlCheckVersion|xmlNewParserCtxt|xmlFreeParserCtxt|xmlCtxtSetResourceLoader|xmlCtxtSetErrorHandler|xmlCtxtReadMemory|xmlFreeDoc|xmlDocGetRootElement|xmlSearchNs|xmlGetLineNo|xmlNewInputFromMemory|xmlSchemaNewParserCtxt|xmlSchemaSetResourceLoader|xmlSchemaSetParserStructuredErrors|xmlSchemaParse|xmlSchemaFreeParserCtxt|xmlSchemaFree|xmlSchemaNewValidCtxt|xmlSchemaFreeValidCtxt|xmlSchemaSetValidStructuredErrors|xmlSchemaSetValidOptions|xmlSchemaValidateDoc' \
  --allowlist-type '_xmlNode|_xmlAttr|_xmlNs|_xmlDoc|_xmlError|xmlElementType|xmlParserOption|xmlSchemaValidOption|xmlResourceType|xmlParserInputFlags|xmlParserErrors|xmlErrorLevel|xmlErrorDomain|xmlStructuredErrorFunc|xmlResourceLoader' \
  --allowlist-var 'LIBXML_VERSION|LIBXML_DOTTED_VERSION' \
  --opaque-type '_xmlDtd|_xmlDict|_xmlParserCtxt|_xmlParserInput|_xmlSchema|_xmlSchemaParserCtxt|_xmlSchemaValidCtxt|_xmlID|_xmlRef' \
  -- -I"$include_dir" -DLIBXML_STATIC
