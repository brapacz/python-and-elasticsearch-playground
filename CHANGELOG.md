# Changelog

## 2023-02-15

* Instead using 3 separated scripts to calculate `with_id`, `without_id` and
  `total` number of artists for each row I used `composite` field and merged
  scripts into one. Source:
  https://github.com/elastic/kibana/issues/126312#issuecomment-1058286604
