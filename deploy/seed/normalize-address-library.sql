-- Normalize overlapping EdgeManager labels for Watchdog's Flow dimensions.
--
-- ISP/operator is a single-valued dimension: one enabled operator may own an
-- ASN. Search-engine labels overlap cloud-provider ASNs, so they are address
-- sets (multi-valued membership), not additional ISP owners.

START TRANSACTION;

INSERT INTO address_sets (
  id, name, description, selector,
  explicit_members, explicit_exclude_members,
  include_set_ids, exclude_set_ids,
  match_direction, enabled
) VALUES
  (
    'a8f15169-0000-4000-8000-000000000001',
    '谷歌搜索',
    '由 EdgeManager 搜索引擎标签转换；按 ASN 15169 匹配，可与云服务主运营商归属重叠。',
    JSON_OBJECT('asns', JSON_ARRAY(15169)),
    JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(),
    'both', 1
  ),
  (
    'a8f08075-0000-4000-8000-000000000002',
    '必应搜索',
    '由 EdgeManager 搜索引擎标签转换；按 ASN 8075 匹配，可与 Azure 主运营商归属重叠。',
    JSON_OBJECT('asns', JSON_ARRAY(8075)),
    JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(),
    'both', 1
  ),
  (
    'a8f38365-0000-4000-8000-000000000003',
    '百度搜索',
    '由 EdgeManager 搜索引擎标签转换；按 ASN 38365 匹配，可与百度云主运营商归属重叠。',
    JSON_OBJECT('asns', JSON_ARRAY(38365)),
    JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(), JSON_ARRAY(),
    'both', 1
  );

UPDATE isp_operators
SET asns = JSON_ARRAY(), enabled = 0, row_version = row_version + 1
WHERE id IN (
  '01M2G0FKEW3Y4K5D6TN7TR2BC4',
  '01M2G0FKEW7NVVS2AWVTJ23JHQ',
  '01M2G0FKEWNN1YVV8SQQXTB4CD'
)
  AND category = 'search';

COMMIT;
