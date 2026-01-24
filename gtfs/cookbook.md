# GTFS Cookbook (for Claude + MotherDuck + Dashboard)

このドキュメントは、Claude（Messages API）に「GTFS特有の作法」を安定して適用させるための参照資料です。
実装の詳細（SQLやPythonコード）は `gtfs/python/*.py` と `gtfs/sql/*.sql` に置き、Claudeには原則として
このCookbookと「安全なツール（tools.py）」だけを使わせます。

## 前提（データ型・単位）
- IDや名称は基本 **文字列**。
- 緯度経度は **float**（WGS84 / EPSG:4326）。
- stop_times の `arrival_time` / `departure_time` は **秒（seconds since midnight）**。
  - 24:00:00 を超える（例: 25:35:00）ことがあるため、**日跨ぎを許容**する。
- 距離（`shape_dist_traveled` など）は **メートル**。

## 必須テーブル（静的GTFS）
- agency, routes, stops, trips, stop_times, calendar, calendar_dates
- shapes（存在する場合は優先して利用）

## サービス日（運行日）の決定ルール
1. 基本は `calendar` による曜日フラグ＋ `start_date`〜`end_date`。
2. `calendar_dates` がある場合、対象日について **例外（追加/削除）を必ず適用**する。
3. ルートや便の分析は `trips.service_id` を起点に、必要な service_id をすべて対象にする（単一 service_id 前提にしない）。

## 時刻の扱い（最重要）
- GTFSの時刻は「サービス日の 00:00 からの経過秒」。
- 24時超え（>= 86400）の時刻は **サービス日の翌日**の時刻として扱う（ただし秒表現は維持する）。
- 便数やヘッドウェイなどの集計では、**同一サービス日内の時間窓**（例: 6:00–9:00）を秒で表してフィルタする。

## 代表停留所（ヘッドウェイ/頻度計算）
- ヘッドウェイ計算は、原則として
  - 同一 `route_id`
  - 同一 `direction_id`
  - 代表的な停留所（一般に `stop_sequence = 1` の最初の停留所）
  を用いて、連続する出発時刻の差から推定する。

## 停留所/路線の曖昧一致（ユーザー入力対応）
- `stop_id` / `stop_name`、`route_id` / `route_short_name` / `route_long_name` を対象に fuzzy matching を行う。
- 括弧書き（例: “(SW Corner)”）は原則無視して比較する。
- 近接停留所検索は 200m 程度をデフォルトにする。

## ジオメトリ（shape）
- 可能なら `shapes` を優先して LineString を生成する。
- shapes がない場合、`stops` と `stop_times` を用いて stop sequence に沿って折れ線を生成する（精度は落ちる）。

## 安全なクエリ/処理の原則（Claudeに守らせる）
- Claudeに生テーブルを直接集計させず、**正規化済みビュー**（例: `gtfs_departures`）を介す。
- クエリは **read-only** を基本とし、行数は LIMIT で制限する（例: 10k 以下）。
- 解析や可視化は「データ抽出 → Pythonで整形 → Vega-Lite/HTML化」の順で行う。

## 推奨の“中間データ”設計（dbt/SQLビュー）
- `gtfs_service_days(date, service_id)`：サービス日展開
- `gtfs_stop_times_norm(...)`：時刻正規化（秒のまま、日跨ぎ許容）
- `gtfs_departures(stop_id, service_date, dep_sec, route_id, trip_id, direction_id)`：発車イベント

## 地図・路線図描画のポイント
- 描画は **Leaflet** を基本に行う。
- 地図背景は OpenStreetMap を利用する。
- 路線図は `shapes` を優先し、ない場合は停留所を結ぶ。
- 停留所は marker で描画し、必要に応じてポップアップやラベルを付与する。
- Vega-Lite は地図路線図描画においては推奨しない（統計グラフなどには利用可）。

### データ量制限の回避（重要）
Motherduck MCPには出力サイズ制限（0MB制限により約540行）があるため、shapesテーブルから大量のデータを取得する際は以下の対策を必ず講じる：

1. **特定路線への絞り込み**：
   - `WHERE shape_id IN (SELECT DISTINCT shape_id FROM trips WHERE route_id = '特定路線ID')`
   - 複数路線でも必要最小限（1〜3路線程度）に絞る

2. **座標点の間引き**：
   - `WHERE shape_pt_sequence % N = 0`（Nは2〜5程度）で等間隔サンプリング
   - または `WHERE shape_pt_sequence <= 100` で上限を設ける

3. **空間範囲での絞り込み**：
   - `WHERE shape_pt_lat BETWEEN lat_min AND lat_max AND shape_pt_lon BETWEEN lon_min AND lon_max`
   - ユーザーが指定した地域や停留所周辺に限定

4. **GROUP BYでの集約**：
   - 詳細な座標が不要な場合、`GROUP BY shape_id` で代表点のみ取得
   - または LineString を直接生成して1行にまとめる

5. **推奨クエリパターン**：
   ```sql
   SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
   FROM shapes
   WHERE shape_id IN (
     SELECT DISTINCT shape_id 
     FROM trips 
     WHERE route_id = '特定路線' 
     LIMIT 3
   )
   AND shape_pt_sequence % 3 = 0  -- 3点に1点を取得
   ORDER BY shape_id, shape_pt_sequence
   LIMIT 500
   ```

6. **代替手段**：
   - shapesが大きすぎる場合、主要停留所（`stop_sequence % 5 = 0`など）を結ぶ簡易路線図に切り替える
   - `trips` と `stop_times` から `stops` の座標を取得する方が軽量な場合がある

---
このCookbookは、生成プロンプト（`generated_prompt.md`）のデータ型・GTFS作法を抽出して整理したものです。
