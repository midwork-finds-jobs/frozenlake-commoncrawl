// SQL macros for structured data on web pages, built on html_select() and the json extension

#include "extract_extension.hpp"

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

namespace {

const DefaultMacro EXTRACT_MACROS[] = {
    // Every schema.org JSON-LD object of the page. Arrays and @graph are flattened, invalid JSON is skipped.
    {DEFAULT_SCHEMA, "json_ld",
     "(html) AS flatten(list_transform("
     "list_filter(list_transform(html_json_ld_raw(html), "
     "lambda s: try_cast(trim(s) AS JSON)), lambda j: j IS NOT NULL), "
     "lambda j: CASE WHEN json_type(j) = 'ARRAY' THEN json_extract(j, '$[*]') "
     "WHEN json_extract(j, '$.\"@graph\"') IS NOT NULL THEN json_extract(j, '$.\"@graph\"[*]') "
     "ELSE [j] END))"},
    // JSON-LD objects of the given schema.org types, for example json_ld_of_type(html, ['JobPosting'])
    {DEFAULT_SCHEMA, "json_ld_of_type",
     "(html, types) AS list_filter(json_ld(html), lambda n: list_has_any("
     "coalesce(try_cast(json_extract(n, '$.\"@type\"') AS VARCHAR[]), [json_extract_string(n, '$.\"@type\"')]), "
     "types))"},
    // Value of a schema.org microdata property, for example microdata(html, 'baseSalary').
    // itemprop can have several space separated properties so ~= is used.
    {DEFAULT_SCHEMA, "microdata",
     "(html, property) AS coalesce("
     "html_select_first(html, '[itemprop~=\"' || css_escape_string(property) || '\"][content]::attr(content)'), "
     "html_select_first(html, '[itemprop~=\"' || css_escape_string(property) || '\"]::text'))"},
    // <meta name="..."> or <meta property="..."> content, for example meta(html, 'og:title')
    {DEFAULT_SCHEMA, "meta",
     "(html, name) AS html_select_first(html, 'meta[name=\"' || css_escape_string(name) || '\" i], "
     "meta[property=\"' || css_escape_string(name) || '\" i]::attr(content)')"},
    // Escapes a value for a double quoted CSS string, for example [title="..."]
    {DEFAULT_SCHEMA, "css_escape_string", "(value) AS replace(replace(value, '\\', '\\\\'), '\"', '\\\"')"},
    // Absolute http(s) URLs of the links on a page, base_url is the URL of the page
    {DEFAULT_SCHEMA, "html_links",
     "(html, base_url) AS list_distinct(list_filter(list_transform(html_select(html, 'a[href]::attr(href)'), "
     "lambda href: url_resolve(base_url, href)), lambda u: u LIKE 'http://%' OR u LIKE 'https://%'))"},
    // Google Analytics, Tag Manager, Ads and AdSense IDs. Pages with the same IDs usually have the same owner.
    {DEFAULT_SCHEMA, "tracking_ids",
     "(html) AS list_distinct(regexp_extract_all(html, "
     "'\\b(UA-[0-9]{4,10}-[0-9]{1,4}|G-[A-Z0-9]{6,12}|GTM-[A-Z0-9]{4,9}|AW-[0-9]{6,12}|ca-pub-[0-9]{10,20})\\b'))"},
    // Email addresses in text, for example emails(html_text(html))
    {DEFAULT_SCHEMA, "emails",
     "(text) AS list_distinct(regexp_extract_all(text, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}'))"},
    // Whether a GTIN (EAN-8, UPC-A, EAN-13 or GTIN-14) has a valid GS1 check digit
    {DEFAULT_SCHEMA, "gtin_valid",
     "(code) AS coalesce(regexp_full_match(code, '[0-9]{8}|[0-9]{12,14}') AND "
     "(10 - list_sum(list_transform(range(1, length(code)), lambda i: (ascii(code[length(code) - i]) - 48) * "
     "CASE WHEN i % 2 = 1 THEN 3 ELSE 1 END)) % 10) % 10 = ascii(code[length(code)]) - 48, false)"},
    // GTINs of a page as 14 digits: numbers next to gtin, ean, barcode or upc anywhere in the HTML, including
    // JSON-LD, embedded JSON and data attributes. Only codes with a valid check digit are kept, and codes which don't
    // identify a product (coupons, placeholders like 0000000000000) are dropped. A listing page has the GTINs of
    // every listed product, product_record() picks the page's own.
    {DEFAULT_SCHEMA, "product_gtins",
     "(html) AS list_sort(list_distinct(list_transform(list_filter(html_gtin_hits(html), "
     "lambda h: NOT list_has_any(gtin_info(h.digits).flags, ['coupon', 'repeated_digits', 'invalid'])), "
     "lambda h: h.gtin)))"},
    // The product of a product page: name, brand, sku, price and its GTIN with a confidence. The main Product of the
    // JSON-LD is the one whose url or @id is the page, else the only one, else the first with offers, and its GTINs
    // are only the ones inside it, so related products on the page don't get mixed in.
    //   high    GTINs of the main Product JSON-LD on a page which isn't a listing
    //   medium  exactly one GTIN on a page with Product JSON-LD or og:type product, or the GTIN closest to the
    //           product id of the url, like 1002730 in /product/1002730/, on a page with several
    //   low     other GTINs on the page, for example a listing or related products
    // The values from the HTML are computed once in the struct outside the lambda so that the HTML is parsed once.
    // Restricted in-store codes and ISBNs are kept, check gtin_info(gtin).flags.
    {DEFAULT_SCHEMA, "product_record",
     "(html, page_url) AS list_transform([struct_pack(ld := json_ld_of_type(html, ['Product', 'ProductGrou"
     "p', 'IndividualProduct']), main := coalesce(list_filter(json_ld_of_type(html, ['Product', 'ProductGr"
     "oup', 'IndividualProduct']), lambda p: rtrim(regexp_replace(regexp_replace(url_resolve(page_url, coa"
     "lesce(p->>'url', p->>'@id')), '^[a-z]+://(www[0-9]*\\.)?', '', 'i'), '[?#].*$', ''), '/') = rtrim(re"
     "gexp_replace(regexp_replace(page_url, '^[a-z]+://(www[0-9]*\\.)?', '', 'i'), '[?#].*$', ''), '/'))[1"
     "], CASE WHEN len(json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct'])) = 1 THEN ("
     "json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']))[1] END, list_filter(json_ld"
     "_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']), lambda p: (p->'offers') IS NOT NUL"
     "L)[1], (json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']))[1]), main_gtins := "
     "coalesce(list_filter(html_gtin_hits((coalesce(list_filter(json_ld_of_type(html, ['Product', 'Product"
     "Group', 'IndividualProduct']), lambda p: rtrim(regexp_replace(regexp_replace(url_resolve(page_url, c"
     "oalesce(p->>'url', p->>'@id')), '^[a-z]+://(www[0-9]*\\.)?', '', 'i'), '[?#].*$', ''), '/') = rtrim("
     "regexp_replace(regexp_replace(page_url, '^[a-z]+://(www[0-9]*\\.)?', '', 'i'), '[?#].*$', ''), '/'))"
     "[1], CASE WHEN len(json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct'])) = 1 THEN"
     " (json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']))[1] END, list_filter(json_"
     "ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']), lambda p: (p->'offers') IS NOT N"
     "ULL)[1], (json_ld_of_type(html, ['Product', 'ProductGroup', 'IndividualProduct']))[1]))::VARCHAR), l"
     "ambda h: NOT list_has_any(gtin_info(h.digits).flags, ['coupon', 'repeated_digits', 'invalid'])), [])"
     ", page_gtins := product_gtins(html), near := html_gtin_near(html, regexp_extract(page_url, '[/=-]([0"
     "-9]{4,})(?:[/?#.-]|$)', 1)), listing := len(json_ld_of_type(html, ['ItemList', 'CollectionPage', 'Se"
     "archResultsPage', 'OfferCatalog'])) > 0, og_title := meta(html, 'og:title'), og_type := meta(html, '"
     "og:type'), h1 := html_select_first(html, 'h1'), meta_price := meta(html, 'product:price:amount'), me"
     "ta_currency := meta(html, 'product:price:currency'))], lambda s: list_transform([list_sort(list_dist"
     "inct(list_transform(s.main_gtins, lambda h: h.gtin)))], lambda main_gtins: struct_pack(name := coale"
     "sce(s.main->>'name', s.og_title, s.h1), brand := coalesce(s.main->'brand'->>'name', s.main->>'brand'"
     "), sku := s.main->>'sku', price := TRY_CAST(coalesce(s.main->'offers'->>'price', s.main->'offers'->0"
     "->>'price', s.main->'offers'->>'lowPrice', s.meta_price) AS DOUBLE), currency := coalesce(s.main->'o"
     "ffers'->>'priceCurrency', s.main->'offers'->0->>'priceCurrency', s.meta_currency), listing := s.list"
     "ing, gtin := CASE WHEN len(main_gtins) = 1 THEN main_gtins[1] WHEN len(main_gtins) = 0 AND len(s.pag"
     "e_gtins) = 1 THEN s.page_gtins[1] WHEN len(main_gtins) = 0 AND NOT s.listing THEN s.near END, gtins "
     ":= CASE WHEN len(main_gtins) > 0 THEN main_gtins ELSE s.page_gtins END, confidence := CASE WHEN len("
     "main_gtins) > 0 AND NOT s.listing THEN 'high' WHEN NOT s.listing AND len(s.page_gtins) = 1 AND (len("
     "s.ld) > 0 OR s.og_type ILIKE '%product%') THEN 'medium' WHEN NOT s.listing AND len(main_gtins) = 0 A"
     "ND len(s.page_gtins) > 1 AND s.near IS NOT NULL THEN 'medium' WHEN len(s.page_gtins) > 0 THEN 'low' "
     "END))[1])[1]"},
    // Splitting one query over many lambdas: the client sends the same SQL with &part=k&parts=n to n lambdas and
    // the lambda sets these variables. in_part(x) is true for 1/n of the values of x, for example
    // WHERE in_part(url) splits warc_record() fetching and HTML parsing. Without parts it is always true.
    {DEFAULT_SCHEMA, "in_part",
     "(value) AS coalesce(getvariable('cc_parts'), 1) <= 1 "
     "OR hash(value) % getvariable('cc_parts') = getvariable('cc_part')"},
    // url_surtkey range of this part. The files are sorted by url_surtkey so every part reads 1/n of the files of
    // every crawl, for example WHERE surtkey_in_part(url_surtkey) splits a scan of whole crawls.
    {DEFAULT_SCHEMA, "surtkey_in_part",
     "(url_surtkey) AS (getvariable('cc_part_low') IS NULL OR url_surtkey >= getvariable('cc_part_low')) "
     "AND (getvariable('cc_part_high') IS NULL OR url_surtkey < getvariable('cc_part_high'))"},
    {nullptr, nullptr, nullptr}};

// Uses surt_prefix() of the frozen catalog
const DefaultTableMacro EXTRACT_TABLE_MACROS[] = {
    // Every capture of one url in every crawl from the CDX index, much faster than url_captures() for the old crawls.
    // With prefix := true every url which starts with page_url, for example 'yle.fi/uutiset/'. Filters on fetch_time
    // and crawl skip the other crawls: WHERE fetch_time BETWEEN '2019-01-01' AND '2019-12-31'. A crawl which couldn't
    // be read is one row with only crawl and error.
    {DEFAULT_SCHEMA,
     "cdx_captures",
     {"page_url", nullptr},
     {{"prefix", "false"}, {nullptr, nullptr}},
     "SELECT crawl, regexp_extract(j->>'filename', '/(warc|crawldiagnostics|robotstxt)/', 1) AS subset, "
     "url_surtkey, j->>'url' AS url, fetch_time, "
     "TRY_CAST(j->>'status' AS SMALLINT) AS fetch_status, j->>'redirect' AS fetch_redirect, "
     "j->>'mime' AS content_mime_type, j->>'mime-detected' AS content_mime_detected, "
     "j->>'languages' AS content_languages, j->>'charset' AS content_charset, j->>'digest' AS content_digest, "
     "j->>'filename' AS warc_filename, TRY_CAST(j->>'offset' AS BIGINT) AS warc_record_offset, "
     "TRY_CAST(j->>'length' AS BIGINT) AS warc_record_length, error "
     "FROM (SELECT *, TRY_CAST(cdx AS JSON) AS j "
     "FROM cdx_scan(CASE WHEN prefix THEN surt_prefix(page_url) ELSE surt_key(page_url) END, prefix)) "
     "ORDER BY url_surtkey, fetch_time"},
    // Versions of a page: the distinct contents (content_digest) with when they were first and last seen
    {DEFAULT_SCHEMA,
     "url_versions",
     {"page_url", nullptr},
     {{nullptr, nullptr}},
     "SELECT content_digest, min(fetch_time) AS first_seen, max(fetch_time) AS last_seen, count(*) AS captures, "
     "arg_min(crawl, fetch_time) AS crawl, arg_min(warc_filename, fetch_time) AS warc_filename, "
     "arg_min(warc_record_offset, fetch_time) AS warc_record_offset, "
     "arg_min(warc_record_length, fetch_time) AS warc_record_length "
     "FROM cdx_captures(page_url) WHERE fetch_status = 200 GROUP BY content_digest ORDER BY first_seen"},
    // The successful capture of a page closest to a time within a year, like the Wayback Machine's timegate
    {DEFAULT_SCHEMA,
     "url_capture_at",
     {"page_url", "fetched_at", nullptr},
     {{nullptr, nullptr}},
     "SELECT * EXCLUDE (error) FROM cdx_captures(page_url) "
     "WHERE fetch_time BETWEEN fetched_at::TIMESTAMP - INTERVAL 1 YEAR AND fetched_at::TIMESTAMP + INTERVAL 1 YEAR "
     "AND fetch_status = 200 ORDER BY abs(epoch(fetch_time) - epoch(fetched_at::TIMESTAMP)) LIMIT 1"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};

} // namespace

void RegisterMacros(ExtensionLoader &loader) {
	for (idx_t index = 0; EXTRACT_MACROS[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(EXTRACT_MACROS[index]);
		loader.RegisterFunction(*info);
	}
	for (idx_t index = 0; EXTRACT_TABLE_MACROS[index].name != nullptr; index++) {
		auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(EXTRACT_TABLE_MACROS[index]);
		loader.RegisterFunction(*info);
	}
}

} // namespace duckdb
