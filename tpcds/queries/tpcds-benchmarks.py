import marimo

__generated_with = "0.11.8"
app = marimo.App(width="full")


@app.cell
def _():
    from datetime import date

    import marimo as mo
    import polars as pl

    return date, mo, pl


@app.cell
def _(pl):
    customer = pl.scan_parquet("data/sf01/customer.parquet")
    customer_address = pl.scan_parquet("data/sf01/customer_address.parquet")
    customer_demographics = pl.scan_parquet("data/sf01/customer_demographics.parquet")
    household_demographics = pl.scan_parquet("data/sf01/household_demographics.parquet")

    item = pl.scan_parquet("data/sf01/item.parquet")
    date_dim = pl.scan_parquet("data/sf01/date_dim.parquet")
    store = pl.scan_parquet("data/sf01/store.parquet")

    store_sales = pl.scan_parquet("data/sf01/store_sales.parquet")
    web_sales = pl.scan_parquet("data/sf01/web_sales.parquet")
    catalog_sales = pl.scan_parquet("data/sf01/catalog_sales.parquet")

    store_returns = pl.scan_parquet("data/sf01/store_returns.parquet")
    web_returns = pl.scan_parquet("data/sf01/web_returns.parquet")
    catalog_returns = pl.scan_parquet("data/sf01/catalog_returns.parquet")
    reason = pl.scan_parquet("data/sf01/reason.parquet")

    web_site = pl.scan_parquet("data/sf01/web_site.parquet")
    catalog_page = pl.scan_parquet("data/sf01/catalog_page.parquet")
    call_center = pl.scan_parquet("data/sf01/call_center.parquet")

    promotion = pl.scan_parquet("data/sf01/promotion.parquet")
    return (
        call_center,
        catalog_page,
        catalog_returns,
        catalog_sales,
        customer,
        customer_address,
        customer_demographics,
        date_dim,
        household_demographics,
        item,
        promotion,
        reason,
        store,
        store_returns,
        store_sales,
        web_returns,
        web_sales,
        web_site,
    )


@app.cell
def _(customer, date_dim, pl, store, store_returns):
    # q1 - done

    def tpcds_q1(store_returns, store, customer):
        customer_total_return = (
            store_returns.join(
                (date_dim.filter(pl.col("d_year") == 2000)),
                left_on="sr_returned_date_sk",
                right_on="d_date_sk",
            )
            .group_by(
                [
                    pl.col("sr_customer_sk").alias("ctr_customer_sk"),
                    pl.col("sr_store_sk").alias("ctr_store_sk"),
                ]
            )
            .agg(
                pl.col("sr_return_amt").cast(pl.Float64).sum().alias("ctr_total_return")
            )
        )

        return (
            customer_total_return.join(
                store.filter(pl.col("s_state") == "TN"),
                left_on="ctr_store_sk",
                right_on="s_store_sk",
            )
            .join(customer, left_on="ctr_customer_sk", right_on="c_customer_sk")
            .with_columns(
                [
                    pl.col("ctr_total_return")
                    .mean()
                    .over("ctr_store_sk")
                    .alias("store_avg_return")
                ]
            )
            .filter(pl.col("ctr_total_return") > pl.col("store_avg_return") * 1.2)
            .select("c_customer_id")
            .sort("c_customer_id")
            .limit(100)
        )

    tpcds_q1(store_returns, store, customer)
    return (tpcds_q1,)


@app.cell
def _(catalog_sales, date_dim, pl, web_sales):
    # q2 - done

    def tpcds_q2(web_sales, catalog_sales, date_dim):
        wscs = pl.concat(
            [
                (
                    web_sales.select(
                        [
                            pl.col("ws_sold_date_sk").alias("sold_date_sk"),
                            pl.col("ws_ext_sales_price").alias("sales_price"),
                        ]
                    )
                ),
                (
                    catalog_sales.select(
                        [
                            pl.col("cs_sold_date_sk").alias("sold_date_sk"),
                            pl.col("cs_ext_sales_price").alias("sales_price"),
                        ]
                    )
                ),
            ]
        )

        wswcs = (
            wscs.join(date_dim, left_on="sold_date_sk", right_on="d_date_sk")
            .group_by("d_week_seq")
            .agg(
                [
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Sunday")
                    .sum()
                    .alias("sun_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Monday")
                    .sum()
                    .alias("mon_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Tuesday")
                    .sum()
                    .alias("tue_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Wednesday")
                    .sum()
                    .alias("wed_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Thursday")
                    .sum()
                    .alias("thu_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Friday")
                    .sum()
                    .alias("fri_sales"),
                    pl.col("sales_price")
                    .filter(pl.col("d_day_name") == "Saturday")
                    .sum()
                    .alias("sat_sales"),
                ]
            )
        )

        y = (
            wswcs.join(
                date_dim.select([pl.col("d_week_seq"), pl.col("d_year")]),
                on="d_week_seq",
            )
            .filter(pl.col("d_year") == 2001)
            .select(
                [
                    pl.col("d_week_seq").alias("d_week_seq1"),
                    pl.col("sun_sales").alias("sun_sales1"),
                    pl.col("mon_sales").alias("mon_sales1"),
                    pl.col("tue_sales").alias("tue_sales1"),
                    pl.col("wed_sales").alias("wed_sales1"),
                    pl.col("thu_sales").alias("thu_sales1"),
                    pl.col("fri_sales").alias("fri_sales1"),
                    pl.col("sat_sales").alias("sat_sales1"),
                ]
            )
        )

        z = (
            wswcs.join(
                date_dim.select([pl.col("d_week_seq"), pl.col("d_year")]),
                on="d_week_seq",
            )
            .filter(pl.col("d_year") == 2002)
            .select(
                [
                    pl.col("d_week_seq").alias("d_week_seq2"),
                    pl.col("sun_sales").alias("sun_sales2"),
                    pl.col("mon_sales").alias("mon_sales2"),
                    pl.col("tue_sales").alias("tue_sales2"),
                    pl.col("wed_sales").alias("wed_sales2"),
                    pl.col("thu_sales").alias("thu_sales2"),
                    pl.col("fri_sales").alias("fri_sales2"),
                    pl.col("sat_sales").alias("sat_sales2"),
                ]
            )
        )

        return (
            y.join(
                z.with_columns([(pl.col("d_week_seq2") - 53).alias("key")]),
                left_on="d_week_seq1",
                right_on="key",
            )
            .select(
                [
                    pl.col("d_week_seq1"),
                    (pl.col("sun_sales1") / pl.col("sun_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r1"),
                    (pl.col("mon_sales1") / pl.col("mon_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r2"),
                    (pl.col("tue_sales1") / pl.col("tue_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r3"),
                    (pl.col("wed_sales1") / pl.col("wed_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r4"),
                    (pl.col("thu_sales1") / pl.col("thu_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r5"),
                    (pl.col("fri_sales1") / pl.col("fri_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r6"),
                    (pl.col("sat_sales1") / pl.col("sat_sales2"))
                    .cast(pl.Float64)
                    .round(2)
                    .alias("r7"),
                ]
            )
            .sort(by="d_week_seq1", nulls_last=False)
        )

    tpcds_q2(web_sales, catalog_sales, date_dim)
    return (tpcds_q2,)


@app.cell
def _(date_dim, item, pl, store_sales):
    # q3 - done

    def tpcds_q3(date_dim, store_sales, item):
        return (
            date_dim.join(store_sales, left_on="d_date_sk", right_on="ss_sold_date_sk")
            .join(item, left_on="ss_item_sk", right_on="i_item_sk")
            .filter((pl.col("i_manufact_id") == 128) & (pl.col("d_moy") == 11))
            .group_by(["d_year", "i_brand_id", "i_brand"])
            .agg(pl.col("ss_ext_sales_price").sum().alias("sum_agg"))
            .rename({"i_brand_id": "brand_id", "i_brand": "brand"})
            .sort(
                by=["d_year", "sum_agg", "brand_id"],
                descending=[False, True, False],
            )
            .limit(100)
        )

    tpcds_q3(date_dim, store_sales, item)
    return (tpcds_q3,)


@app.cell
def _(catalog_sales, customer, date_dim, pl, store_sales, web_sales):
    # q4 - done

    def tpcds_q4(customer, store_sales, date_dim, catalog_sales, web_sales):
        store_year_total = (
            customer.join(
                store_sales, left_on="c_customer_sk", right_on="ss_customer_sk"
            )
            .join(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .group_by(
                [
                    pl.col("c_customer_id"),
                    pl.col("c_first_name"),
                    pl.col("c_last_name"),
                    pl.col("c_preferred_cust_flag"),
                    pl.col("c_birth_country"),
                    pl.col("c_login"),
                    pl.col("c_email_address"),
                    pl.col("d_year"),
                ]
            )
            .agg(
                [
                    (
                        (
                            pl.col("ss_ext_list_price")
                            - (pl.col("ss_ext_wholesale_cost"))
                            - (pl.col("ss_ext_discount_amt"))
                            + pl.col("ss_ext_sales_price")
                        )
                        / 2
                    )
                    .sum()
                    .alias("year_total")
                ]
            )
            .with_columns(pl.lit("s").alias("sale_type"))
            .rename({"d_year": "dyear"})
        )

        catalog_year_total = (
            customer.join(
                catalog_sales,
                left_on="c_customer_sk",
                right_on="cs_bill_customer_sk",
            )
            .join(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")
            .group_by(
                [
                    pl.col("c_customer_id"),
                    pl.col("c_first_name"),
                    pl.col("c_last_name"),
                    pl.col("c_preferred_cust_flag"),
                    pl.col("c_birth_country"),
                    pl.col("c_login"),
                    pl.col("c_email_address"),
                    pl.col("d_year"),
                ]
            )
            .agg(
                [
                    (
                        (
                            pl.col("cs_ext_list_price")
                            - (pl.col("cs_ext_wholesale_cost"))
                            - (pl.col("cs_ext_discount_amt"))
                            + pl.col("cs_ext_sales_price")
                        )
                        / 2
                    )
                    .sum()
                    .alias("year_total")
                ]
            )
            .with_columns(pl.lit("c").alias("sale_type"))
            .rename({"d_year": "dyear"})
        )

        web_year_total = (
            customer.join(
                web_sales, left_on="c_customer_sk", right_on="ws_bill_customer_sk"
            )
            .join(date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk")
            .group_by(
                [
                    pl.col("c_customer_id"),
                    pl.col("c_first_name"),
                    pl.col("c_last_name"),
                    pl.col("c_preferred_cust_flag"),
                    pl.col("c_birth_country"),
                    pl.col("c_login"),
                    pl.col("c_email_address"),
                    pl.col("d_year"),
                ]
            )
            .agg(
                [
                    (
                        (
                            pl.col("ws_ext_list_price")
                            - (pl.col("ws_ext_wholesale_cost"))
                            - (pl.col("ws_ext_discount_amt"))
                            + pl.col("ws_ext_sales_price")
                        )
                        / 2
                    )
                    .sum()
                    .alias("year_total")
                ]
            )
            .with_columns(pl.lit("w").alias("sale_type"))
            .rename({"d_year": "dyear"})
        )

        year_total = pl.concat(
            [store_year_total, catalog_year_total, web_year_total]
        ).select(
            [
                pl.col("c_customer_id").alias("customer_id"),
                pl.col("c_first_name").alias("customer_first_name"),
                pl.col("c_last_name").alias("customer_last_name"),
                pl.col("c_preferred_cust_flag").alias("customer_preferred_cust_flag"),
                pl.col("c_birth_country").alias("customer_birth_country"),
                pl.col("c_login").alias("customer_login"),
                pl.col("c_email_address").alias("customer_email_address"),
                pl.col("dyear"),
                pl.col("year_total"),
                pl.col("sale_type"),
            ]
        )

        t_s_firstyear = year_total.filter(
            (pl.col("sale_type") == "s") & (pl.col("dyear") == 2001)
        )
        t_s_secyear = year_total.filter(
            (pl.col("sale_type") == "s") & (pl.col("dyear") == 2002)
        )
        t_c_firstyear = year_total.filter(
            (pl.col("sale_type") == "c") & (pl.col("dyear") == 2001)
        )
        t_c_secyear = year_total.filter(
            (pl.col("sale_type") == "c") & (pl.col("dyear") == 2002)
        )
        t_w_firstyear = year_total.filter(
            (pl.col("sale_type") == "w") & (pl.col("dyear") == 2001)
        )
        t_w_secyear = year_total.filter(
            (pl.col("sale_type") == "w") & (pl.col("dyear") == 2002)
        )

        return (
            t_s_secyear.join(t_s_firstyear, on="customer_id", suffix="_s_first")
            .join(t_c_secyear, on="customer_id", suffix="_c_sec")
            .join(t_c_firstyear, on="customer_id", suffix="_c_first")
            .join(t_w_firstyear, on="customer_id", suffix="_w_first")
            .join(t_w_secyear, on="customer_id", suffix="_w_sec")
            .filter(
                (pl.col("year_total_s_first") > 0),
                (pl.col("year_total_c_first") > 0),
                (pl.col("year_total_w_first") > 0),
                (
                    pl.when(pl.col("year_total_c_first") > 0)
                    .then(pl.col("year_total_c_sec") / pl.col("year_total_c_first"))
                    .otherwise(None)
                    > pl.when(pl.col("year_total_s_first") > 0)
                    .then(pl.col("year_total") / pl.col("year_total_s_first"))
                    .otherwise(None)
                ),
                (
                    pl.when(pl.col("year_total_c_first") > 0)
                    .then(pl.col("year_total_c_sec") / pl.col("year_total_c_first"))
                    .otherwise(None)
                    > pl.when(pl.col("year_total_w_first") > 0)
                    .then(pl.col("year_total_w_sec") / pl.col("year_total_w_first"))
                    .otherwise(None)
                ),
            )
            .select(
                [
                    pl.col("customer_id"),
                    pl.col("customer_first_name"),
                    pl.col("customer_last_name"),
                    pl.col("customer_preferred_cust_flag"),
                ]
            )
            .sort(
                by=[
                    "customer_id",
                    "customer_first_name",
                    "customer_last_name",
                    "customer_preferred_cust_flag",
                ],
                nulls_last=False,
            )
            .limit(100)
        )

    tpcds_q4(customer, store_sales, date_dim, catalog_sales, web_sales).collect()
    return (tpcds_q4,)


@app.cell
def _(
    catalog_page,
    catalog_returns,
    catalog_sales,
    date,
    date_dim,
    pl,
    store,
    store_returns,
    store_sales,
    web_returns,
    web_sales,
    web_site,
):
    # q5 - done

    def tpcds_q5(
        store_sales,
        store_returns,
        date_dim,
        store,
        catalog_sales,
        catalog_returns,
        web_sales,
        web_returns,
    ):
        DATE_MIN = date(2000, 8, 23)
        DATE_MAX = date(2000, 9, 6)

        s1 = store_sales.select(
            [
                pl.col("ss_store_sk").alias("store_sk"),
                pl.col("ss_sold_date_sk").alias("date_sk"),
                pl.col("ss_ext_sales_price").alias("sales_price"),
                pl.col("ss_net_profit").alias("profit"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("return_amt"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("net_loss"),
            ]
        )

        s2 = store_returns.select(
            [
                pl.col("sr_store_sk").alias("store_sk"),
                pl.col("sr_returned_date_sk").alias("date_sk"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("sales_price"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("profit"),
                pl.col("sr_return_amt").alias("return_amt"),
                pl.col("sr_net_loss").alias("net_loss"),
            ]
        )

        store_channel = (
            pl.concat([s1, s2])
            .join(
                date_dim.select([pl.col("d_date_sk"), pl.col("d_date")]),
                left_on="date_sk",
                right_on="d_date_sk",
            )
            .filter(pl.col("d_date") >= DATE_MIN, pl.col("d_date") <= DATE_MAX)
            .join(
                store.select([pl.col("s_store_sk"), pl.col("s_store_id")]),
                left_on="store_sk",
                right_on="s_store_sk",
            )
            .group_by(pl.col("s_store_id"))
            .agg(
                pl.col("sales_price").sum().alias("sales"),
                pl.col("profit").sum().alias("profit"),
                pl.col("return_amt").sum().alias("returns_"),
                pl.col("net_loss").sum().alias("profit_loss"),
            )
        ).select(
            [
                pl.lit("store_channel").alias("channel"),
                (pl.lit("store") + pl.col("s_store_id")).alias("id"),
                pl.col("sales"),
                pl.col("returns_"),
                (pl.col("profit") - pl.col("profit_loss")).alias("profit"),
            ]
        )

        c1 = catalog_sales.select(
            [
                pl.col("cs_catalog_page_sk").alias("page_sk"),
                pl.col("cs_sold_date_sk").alias("date_sk"),
                pl.col("cs_ext_sales_price").alias("sales_price"),
                pl.col("cs_net_profit").alias("profit"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("return_amt"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("net_loss"),
            ]
        )

        c2 = catalog_returns.select(
            [
                pl.col("cr_catalog_page_sk").alias("page_sk"),
                pl.col("cr_returned_date_sk").alias("date_sk"),
                # For the returns records, set sales_price and profit to 0
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("sales_price"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("profit"),
                pl.col("cr_return_amount").alias("return_amt"),
                pl.col("cr_net_loss").alias("net_loss"),
            ]
        )

        catalog_channel = (
            pl.concat([c1, c2])
            .join(
                date_dim.select([pl.col("d_date_sk"), pl.col("d_date")]),
                left_on="date_sk",
                right_on="d_date_sk",
            )
            .filter(pl.col("d_date") >= DATE_MIN, pl.col("d_date") <= DATE_MAX)
            .join(
                catalog_page.select(
                    [pl.col("cp_catalog_page_sk"), pl.col("cp_catalog_page_id")]
                ),
                left_on="page_sk",
                right_on="cp_catalog_page_sk",
            )
            .group_by(pl.col("cp_catalog_page_id"))
            .agg(
                [
                    pl.col("sales_price").sum().alias("sales"),
                    pl.col("profit").sum().alias("profit"),
                    pl.col("return_amt").sum().alias("returns_"),
                    pl.col("net_loss").sum().alias("profit_loss"),
                ]
            )
            .select(
                [
                    pl.lit("catalog channel").alias("channel"),
                    (pl.lit("catalog_page") + pl.col("cp_catalog_page_id")).alias("id"),
                    pl.col("sales"),
                    pl.col("returns_"),
                    (pl.col("profit") - pl.col("profit_loss")).alias("profit"),
                ]
            )
        )

        w1 = web_sales.select(
            [
                pl.col("ws_web_site_sk").alias("web_site_sk"),
                pl.col("ws_sold_date_sk").alias("date_sk"),
                pl.col("ws_ext_sales_price").alias("sales_price"),
                pl.col("ws_net_profit").alias("profit"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("return_amt"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("net_loss"),
            ]
        )

        w2 = web_returns.join(
            web_sales.select(
                [
                    pl.col("ws_item_sk"),
                    pl.col("ws_order_number"),
                    pl.col("ws_web_site_sk"),
                ]
            ),
            left_on=["wr_item_sk", "wr_order_number"],
            right_on=["ws_item_sk", "ws_order_number"],
            how="left",
        ).select(
            [
                pl.col("ws_web_site_sk").alias("web_site_sk"),
                pl.col("wr_returned_date_sk").alias("date_sk"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("sales_price"),
                pl.lit(0).cast(pl.Decimal(None, 2)).alias("profit"),
                pl.col("wr_return_amt").alias("return_amt"),
                pl.col("wr_net_loss").alias("net_loss"),
            ]
        )

        web_channel = (
            pl.concat([w1, w2])
            .join(
                date_dim.select([pl.col("d_date_sk"), pl.col("d_date")]),
                left_on="date_sk",
                right_on="d_date_sk",
            )
            .filter(pl.col("d_date") >= DATE_MIN, pl.col("d_date") <= DATE_MAX)
            .join(
                web_site.select([pl.col("web_site_sk"), pl.col("web_site_id")]),
                on="web_site_sk",
            )
            .group_by(pl.col("web_site_id"))
            .agg(
                pl.col("sales_price").sum().alias("sales"),
                pl.col("profit").sum().alias("profit"),
                pl.col("return_amt").sum().alias("returns_"),
                pl.col("net_loss").sum().alias("profit_loss"),
            )
            .select(
                [
                    pl.lit("web channel").alias("channel"),
                    (pl.lit("web_site") + pl.col("web_site_id")).alias("id"),
                    pl.col("sales"),
                    pl.col("returns_"),
                    (pl.col("profit") - pl.col("profit_loss")).alias("profit"),
                ]
            )
        )

        channels = pl.concat([store_channel, catalog_channel, web_channel])

        detail = channels.group_by(["channel", "id"]).agg(
            [
                pl.col("sales").cast(pl.Float64).sum().alias("sales"),
                pl.col("returns_").cast(pl.Float64).sum().alias("returns_"),
                pl.col("profit").cast(pl.Float64).sum().alias("profit"),
            ]
        )

        by_channel = (
            channels.group_by(["channel"])
            .agg(
                [
                    pl.col("sales").cast(pl.Float64).sum().alias("sales"),
                    pl.col("returns_").cast(pl.Float64).sum().alias("returns_"),
                    pl.col("profit").cast(pl.Float64).sum().alias("profit"),
                ]
            )
            .select(
                [
                    pl.col("channel"),
                    pl.lit(None).alias("id"),
                    pl.col("sales"),
                    pl.col("returns_"),
                    pl.col("profit"),
                ]
            )
        )

        total = channels.select(
            [
                pl.col("sales").cast(pl.Float64).sum().alias("sales"),
                pl.col("returns_").cast(pl.Float64).sum().alias("returns_"),
                pl.col("profit").cast(pl.Float64).sum().alias("profit"),
            ]
        ).select(
            [
                pl.lit(None).alias("channel"),
                pl.lit(None).alias("id"),
                pl.col("sales"),
                pl.col("returns_"),
                pl.col("profit"),
            ]
        )

        return (
            pl.concat([detail, by_channel, total])
            .sort(by=[pl.col("channel"), pl.col("id")], descending=[False, False])
            .limit(100)
        )

    tpcds_q5(
        store_sales,
        store_returns,
        date_dim,
        store,
        catalog_sales,
        catalog_returns,
        web_sales,
        web_returns,
    )
    return (tpcds_q5,)


@app.cell
def _(customer, customer_address, date_dim, item, pl, store_sales):
    # q6 - done

    def tpcds_q6(customer_address, customer, date_dim, item):
        return (
            customer_address.join(
                customer, left_on="ca_address_sk", right_on="c_current_addr_sk"
            )
            .join(store_sales, left_on="c_customer_sk", right_on="ss_customer_sk")
            .join(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .join(item, left_on="ss_item_sk", right_on="i_item_sk")
            .join(
                date_dim.filter(pl.col("d_year") == 2001, pl.col("d_moy") == 1),
                on="d_month_seq",
                how="semi",
            )
            .join(
                item.join(
                    item.group_by(pl.col("i_category")).agg(
                        pl.col("i_current_price")
                        .cast(pl.Float64)
                        .mean()
                        .alias("avg_price_cat")
                    ),
                    on="i_category",
                    how="left",
                ).filter(pl.col("i_current_price") > 1.2 * pl.col("avg_price_cat")),
                left_on="ss_item_sk",
                right_on="i_item_sk",
            )
            .group_by(pl.col("ca_state"))
            .agg(pl.len().alias("cnt"))
            .filter(pl.col("cnt") >= 10)
            .sort(by=["cnt", "ca_state"], descending=False, nulls_last=False)
            .limit(100)
        )

    tpcds_q6(customer_address, customer, date_dim, item)
    return (tpcds_q6,)


@app.cell
def _(customer_demographics, date_dim, item, pl, promotion, store_sales):
    # q7 - done

    def tpcds_q7(store_sales, item, customer_demographics, promotion):
        return (
            store_sales.join(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .join(item, left_on="ss_item_sk", right_on="i_item_sk")
            .join(customer_demographics, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
            .join(promotion, left_on="ss_promo_sk", right_on="p_promo_sk")
            .filter(
                pl.col("cd_gender") == "M",
                pl.col("cd_marital_status") == "S",
                pl.col("cd_education_status") == "College",
                pl.col("d_year") == 2000,
                (pl.col("p_channel_email") == "N") | (pl.col("p_channel_event") == "N"),
            )
            .group_by(pl.col("i_item_id"))
            .agg(
                [
                    pl.col("ss_quantity").cast(pl.Float64).mean().alias("agg1"),
                    pl.col("ss_list_price").cast(pl.Float64).mean().alias("agg2"),
                    pl.col("ss_coupon_amt").cast(pl.Float64).mean().alias("agg3"),
                    pl.col("ss_sales_price").cast(pl.Float64).mean().alias("agg4"),
                ]
            )
            .sort(by="i_item_id")
            .limit(100)
        )

    tpcds_q7(store_sales, item, customer_demographics, promotion)
    return (tpcds_q7,)


@app.cell
def _(customer, customer_address, date_dim, pl, store, store_sales):
    # q8 - done

    def tpcds_q8(customer_address, customer, store_sales, date_dim):
        zip_codes = [
            "24128",
            "76232",
            "65084",
            "87816",
            "83926",
            "77556",
            "20548",
            "26231",
            "43848",
            "15126",
            "91137",
            "61265",
            "98294",
            "25782",
            "17920",
            "18426",
            "98235",
            "40081",
            "84093",
            "28577",
            "55565",
            "17183",
            "54601",
            "67897",
            "22752",
            "86284",
            "18376",
            "38607",
            "45200",
            "21756",
            "29741",
            "96765",
            "23932",
            "89360",
            "29839",
            "25989",
            "28898",
            "91068",
            "72550",
            "10390",
            "18845",
            "47770",
            "82636",
            "41367",
            "76638",
            "86198",
            "81312",
            "37126",
            "39192",
            "88424",
            "72175",
            "81426",
            "53672",
            "10445",
            "42666",
            "66864",
            "66708",
            "41248",
            "48583",
            "82276",
            "18842",
            "78890",
            "49448",
            "14089",
            "38122",
            "34425",
            "79077",
            "19849",
            "43285",
            "39861",
            "66162",
            "77610",
            "13695",
            "99543",
            "83444",
            "83041",
            "12305",
            "57665",
            "68341",
            "25003",
            "57834",
            "62878",
            "49130",
            "81096",
            "18840",
            "27700",
            "23470",
            "50412",
            "21195",
            "16021",
            "76107",
            "71954",
            "68309",
            "18119",
            "98359",
            "64544",
            "10336",
            "86379",
            "27068",
            "39736",
            "98569",
            "28915",
            "24206",
            "56529",
            "57647",
            "54917",
            "42961",
            "91110",
            "63981",
            "14922",
            "36420",
            "23006",
            "67467",
            "32754",
            "30903",
            "20260",
            "31671",
            "51798",
            "72325",
            "85816",
            "68621",
            "13955",
            "36446",
            "41766",
            "68806",
            "16725",
            "15146",
            "22744",
            "35850",
            "88086",
            "51649",
            "18270",
            "52867",
            "39972",
            "96976",
            "63792",
            "11376",
            "94898",
            "13595",
            "10516",
            "90225",
            "58943",
            "39371",
            "94945",
            "28587",
            "96576",
            "57855",
            "28488",
            "26105",
            "83933",
            "25858",
            "34322",
            "44438",
            "73171",
            "30122",
            "34102",
            "22685",
            "71256",
            "78451",
            "54364",
            "13354",
            "45375",
            "40558",
            "56458",
            "28286",
            "45266",
            "47305",
            "69399",
            "83921",
            "26233",
            "11101",
            "15371",
            "69913",
            "35942",
            "15882",
            "25631",
            "24610",
            "44165",
            "99076",
            "33786",
            "70738",
            "26653",
            "14328",
            "72305",
            "62496",
            "22152",
            "10144",
            "64147",
            "48425",
            "14663",
            "21076",
            "18799",
            "30450",
            "63089",
            "81019",
            "68893",
            "24996",
            "51200",
            "51211",
            "45692",
            "92712",
            "70466",
            "79994",
            "22437",
            "25280",
            "38935",
            "71791",
            "73134",
            "56571",
            "14060",
            "19505",
            "72425",
            "56575",
            "74351",
            "68786",
            "51650",
            "20004",
            "18383",
            "76614",
            "11634",
            "18906",
            "15765",
            "41368",
            "73241",
            "76698",
            "78567",
            "97189",
            "28545",
            "76231",
            "75691",
            "22246",
            "51061",
            "90578",
            "56691",
            "68014",
            "51103",
            "94167",
            "57047",
            "14867",
            "73520",
            "15734",
            "63435",
            "25733",
            "35474",
            "24676",
            "94627",
            "53535",
            "17879",
            "15559",
            "53268",
            "59166",
            "11928",
            "59402",
            "33282",
            "45721",
            "43933",
            "68101",
            "33515",
            "36634",
            "71286",
            "19736",
            "58058",
            "55253",
            "67473",
            "41918",
            "19515",
            "36495",
            "19430",
            "22351",
            "77191",
            "91393",
            "49156",
            "50298",
            "87501",
            "18652",
            "53179",
            "18767",
            "63193",
            "23968",
            "65164",
            "68880",
            "21286",
            "72823",
            "58470",
            "67301",
            "13394",
            "31016",
            "70372",
            "67030",
            "40604",
            "24317",
            "45748",
            "39127",
            "26065",
            "77721",
            "31029",
            "31880",
            "60576",
            "24671",
            "45549",
            "13376",
            "50016",
            "33123",
            "19769",
            "22927",
            "97789",
            "46081",
            "72151",
            "15723",
            "46136",
            "51949",
            "68100",
            "96888",
            "64528",
            "14171",
            "79777",
            "28709",
            "11489",
            "25103",
            "32213",
            "78668",
            "22245",
            "15798",
            "27156",
            "37930",
            "62971",
            "21337",
            "51622",
            "67853",
            "10567",
            "38415",
            "15455",
            "58263",
            "42029",
            "60279",
            "37125",
            "56240",
            "88190",
            "50308",
            "26859",
            "64457",
            "89091",
            "82136",
            "62377",
            "36233",
            "63837",
            "58078",
            "17043",
            "30010",
            "60099",
            "28810",
            "98025",
            "29178",
            "87343",
            "73273",
            "30469",
            "64034",
            "39516",
            "86057",
            "21309",
            "90257",
            "67875",
            "40162",
            "11356",
            "73650",
            "61810",
            "72013",
            "30431",
            "22461",
            "19512",
            "13375",
            "55307",
            "30625",
            "83849",
            "68908",
            "26689",
            "96451",
            "38193",
            "46820",
            "88885",
            "84935",
            "69035",
            "83144",
            "47537",
            "56616",
            "94983",
            "48033",
            "69952",
            "25486",
            "61547",
            "27385",
            "61860",
            "58048",
            "56910",
            "16807",
            "17871",
            "35258",
            "31387",
            "35458",
            "35576",
        ]

        sub = (
            customer_address.filter(pl.col("ca_zip").str.slice(0, 5).is_in(zip_codes))
            .join(
                customer.filter(pl.col("c_preferred_cust_flag") == "Y"),
                left_on="ca_address_sk",
                right_on="c_current_addr_sk",
            )
            .group_by(pl.col("ca_zip"))
            .agg(pl.col("ca_zip").count().alias("count"))
            .filter(pl.col("count") > 10)
        )

        return (
            store_sales.join(
                date_dim.filter((pl.col("d_qoy") == 2) & (pl.col("d_year") == 1998)),
                left_on="ss_sold_date_sk",
                right_on="d_date_sk",
            )
            .join(store, left_on="ss_store_sk", right_on="s_store_sk")
            .join(
                sub,
                left_on=(pl.col("s_zip").str.slice(0, 2)),
                right_on=(pl.col("ca_zip").str.slice(0, 2)),
                how="inner",
            )
            .group_by(pl.col("s_store_name"))
            .agg(pl.col("ss_net_profit").sum().alias("net_profit"))
            .sort(by="s_store_name")
        )

    tpcds_q8(customer_address, customer, store_sales, date_dim)
    return (tpcds_q8,)


@app.cell
def _(pl, store_sales):
    # q9 done // note: doesn't seem to be any relation
    # between reason and store_sales. result is same as SQL output

    def tpcds_q9(store_sales):
        return pl.concat(
            [
                store_sales.filter(pl.col("ss_quantity").is_between(1, 20)).select(
                    pl.when(pl.len() > 74129)
                    .then(pl.mean("ss_ext_discount_amt"))
                    .otherwise(pl.mean("ss_net_paid"))
                    .alias("bucket1")
                ),
                store_sales.filter(pl.col("ss_quantity").is_between(21, 40)).select(
                    pl.when(pl.len() > 122840)
                    .then(pl.mean("ss_ext_discount_amt"))
                    .otherwise(pl.mean("ss_net_paid"))
                    .alias("bucket2")
                ),
                store_sales.filter(pl.col("ss_quantity").is_between(41, 60)).select(
                    pl.when(pl.len() > 56580)
                    .then(pl.mean("ss_ext_discount_amt"))
                    .otherwise(pl.mean("ss_net_paid"))
                    .alias("bucket3")
                ),
                store_sales.filter(pl.col("ss_quantity").is_between(61, 80)).select(
                    pl.when(pl.len() > 10097)
                    .then(pl.mean("ss_ext_discount_amt"))
                    .otherwise(pl.mean("ss_net_paid"))
                    .alias("bucket4")
                ),
                store_sales.filter(pl.col("ss_quantity").is_between(81, 100)).select(
                    pl.when(pl.len() > 165306)
                    .then(pl.mean("ss_ext_discount_amt"))
                    .otherwise(pl.mean("ss_net_paid"))
                    .alias("bucket5")
                ),
            ],
            how="horizontal",
        )

    tpcds_q9(store_sales)
    return (tpcds_q9,)


@app.cell
def _(
    catalog_sales,
    customer,
    customer_address,
    customer_demographics,
    date_dim,
    pl,
    s,
    store_sales,
    web_sales,
):
    # q10 - done

    def tpcds_q10(
        date_dim,
        store_sales,
        web_sales,
        catalog_sales,
        customer,
        customer_address,
        customer_demographics,
    ):
        date_filter_20020104 = date_dim.filter(
            (pl.col("d_year") == 2002), (pl.col("d_moy").is_between(1, 1 + 3))
        )

        store_customers = store_sales.join(
            date_filter_20020104, left_on="ss_sold_date_sk", right_on="d_date_sk"
        ).select(pl.col("ss_customer_sk"))

        web_customers = web_sales.join(
            date_filter_20020104, left_on="ws_sold_date_sk", right_on="d_date_sk"
        ).select("ws_bill_customer_sk")

        catalog_customers = catalog_sales.join(
            date_filter_20020104, left_on="cs_sold_date_sk", right_on="d_date_sk"
        ).select("cs_ship_customer_sk")

        return (
            customer.join(
                customer_address,
                left_on="c_current_addr_sk",
                right_on="ca_address_sk",
            )
            .filter(
                pl.col("ca_county").is_in(
                    [
                        "Rush County",
                        "Toole County",
                        "Jefferson County",
                        "Dona Ana County",
                        "La Porte County",
                    ]
                )
            )
            .join(
                customer_demographics,
                left_on="c_current_cdemo_sk",
                right_on="cd_demo_sk",
            )
            .join(
                store_customers,
                left_on="c_customer_sk",
                right_on="ss_customer_sk",
                how="semi",
            )
            .join(
                pl.concat(
                    [
                        web_customers.rename(
                            {"ws_bill_customer_sk": "online_customer_sk"}
                        ),
                        catalog_customers.rename(
                            {"cs_ship_customer_sk": "online_customer_sk"}
                        ),
                    ]
                ),
                left_on="c_customer_sk",
                right_on="online_customer_sk",
                how="semi",
            )
            .group_by(
                [
                    pl.col("cd_gender"),
                    pl.col("cd_marital_status"),
                    pl.col("cd_education_status"),
                    pl.col("cd_purchase_estimate"),
                    pl.col("cd_credit_rating"),
                    pl.col("cd_dep_count"),
                    pl.col("cd_dep_employed_count"),
                    pl.col("cd_dep_college_count"),
                ]
            )
            .agg([pl.len().alias("count")])
            .select(
                [
                    "cd_gender",
                    "cd_marital_status",
                    "cd_education_status",
                    pl.col("count").alias("cnt1"),
                    "cd_purchase_estimate",
                    pl.col("count").alias("cnt2"),
                    "cd_credit_rating",
                    pl.col("count").alias("cnt3"),
                    "cd_dep_count",
                    pl.col("count").alias("cnt4"),
                    "cd_dep_employed_count",
                    pl.col("count").alias("cnt5"),
                    "cd_dep_college_count",
                    pl.col("count").alias("cnt6"),
                ]
            )
            .sort(
                [
                    "cd_gender",
                    "cd_marital_status",
                    "cd_education_status",
                    "cd_purchase_estimate",
                    "cd_credit_rating",
                    "cd_dep_count",
                    "cd_dep_employed_count",
                    "cd_dep_college_count",
                ]
            )
            .limit(100)
        )

    tpcds_q10(
        date_dim,
        store_sales,
        web_sales,
        catalog_sales,
        customer,
        customer_address,
        customer_demographics,
    )
    return (tpcds_q10,)


@app.cell
def _():
    # q11
    return


@app.cell
def _(date, date_dim, item, pl, web_sales):
    # q12 - done

    def tpcds_q12(web_sales, item, date_dim):
        return (
            web_sales.join(item, left_on="ws_item_sk", right_on="i_item_sk")
            .join(date_dim, left_on="ws_sold_date_sk", right_on="d_date_sk")
            .filter(
                pl.col("i_category").is_in(["Sports", "Books", "Home"]),
                pl.col("d_date").is_between(date(1999, 2, 22), date(1999, 3, 24)),
            )
            .group_by(
                [
                    pl.col("i_item_id"),
                    pl.col("i_item_desc"),
                    pl.col("i_category"),
                    pl.col("i_class"),
                    pl.col("i_current_price"),
                ]
            )
            .agg(
                pl.col("ws_ext_sales_price").sum().alias("itemrevenue").cast(pl.Float64)
            )
            .with_columns(
                [
                    (
                        (pl.col("itemrevenue") * 100.0000)
                        / pl.col("itemrevenue").sum().over("i_class")
                    ).alias("revenueratio")
                ]
            )
            .sort(
                by=[
                    "i_category",
                    "i_class",
                    "i_item_id",
                    "i_item_desc",
                    "revenueratio",
                ]
            )
            .limit(100)
        )

    tpcds_q12(web_sales, item, date_dim)
    return (tpcds_q12,)


@app.cell
def _(
    customer_address,
    customer_demographics,
    date_dim,
    household_demographics,
    pl,
    store,
    store_sales,
):
    # q13 - done

    def tpcds_q13(
        store_sales,
        store,
        household_demographics,
        customer_demographics,
        customer_address,
        date_dim,
    ):
        return (
            store_sales.join(store, left_on="ss_store_sk", right_on="s_store_sk")
            .join(
                household_demographics,
                left_on="ss_hdemo_sk",
                right_on="hd_demo_sk",
            )
            .join(customer_demographics, left_on="ss_cdemo_sk", right_on="cd_demo_sk")
            .join(customer_address, left_on="ss_addr_sk", right_on="ca_address_sk")
            .join(date_dim, left_on="ss_sold_date_sk", right_on="d_date_sk")
            .filter(
                (pl.col("ca_country") == "United States")
                & (pl.col("d_year") == 2001)
                & (
                    (
                        (pl.col("cd_marital_status") == "M")
                        & (pl.col("cd_education_status") == "Advanced Degree")
                        & pl.col("ss_sales_price").is_between(100.00, 150.00)
                        & (pl.col("hd_dep_count") == 3)
                    )
                    | (
                        (pl.col("cd_marital_status") == "S")
                        & (pl.col("cd_education_status") == "College")
                        & pl.col("ss_sales_price").is_between(50.00, 100.00)
                        & (pl.col("hd_dep_count") == 1)
                    )
                    | (
                        (pl.col("cd_marital_status") == "W")
                        & (pl.col("cd_education_status") == "2 yr Degree")
                        & pl.col("ss_sales_price").is_between(150.00, 200.00)
                        & (pl.col("hd_dep_count") == 1)
                    )
                ),
                (
                    (pl.col("ca_state").is_in(["TX", "OH", "TX"]))
                    & (pl.col("ss_net_profit").is_between(100, 200))
                    | (pl.col("ca_state").is_in(["OR", "NM", "KY"]))
                    & (pl.col("ss_net_profit").is_between(150, 300))
                    | (pl.col("ca_state").is_in(["VA", "TX", "MS"]))
                    & (pl.col("ss_net_profit").is_between(50, 250))
                ),
            )
            .select(
                pl.col("ss_quantity").cast(pl.Float64).mean().alias("avg1"),
                pl.col("ss_ext_sales_price").cast(pl.Float64).mean().alias("avg2"),
                pl.col("ss_ext_wholesale_cost").cast(pl.Float64).mean().alias("avg3"),
                pl.col("ss_ext_wholesale_cost").cast(pl.Float64).sum().alias("sum1"),
            )
        )

    tpcds_q13(
        store_sales,
        store,
        household_demographics,
        customer_demographics,
        customer_address,
        date_dim,
    )
    return (tpcds_q13,)


@app.cell
def _():
    # q14
    return


@app.cell
def _(catalog_sales, customer, customer_address, date_dim, pl):
    # q15 - done

    def tpcds_q15(catalog_sales, customer, customer_address):
        return (
            catalog_sales.join(
                customer, left_on="cs_bill_customer_sk", right_on="c_customer_sk"
            )
            .join(
                customer_address,
                left_on="c_current_addr_sk",
                right_on="ca_address_sk",
            )
            .join(date_dim, left_on="cs_sold_date_sk", right_on="d_date_sk")
            .filter(
                (
                    pl.col("ca_zip")
                    .str.slice(0, 5)
                    .is_in(
                        [
                            "85669",
                            "86197",
                            "88274",
                            "83405",
                            "86475",
                            "85392",
                            "85460",
                            "80348",
                            "81792",
                        ]
                    )
                )
                | (pl.col("ca_state").is_in(["CA", "WA", "GA"]))
                | (pl.col("cs_sales_price") > 500),
                pl.col("d_qoy") == 2,
                pl.col("d_year") == 2001,
            )
            .group_by(pl.col("ca_zip"))
            .agg(pl.col("cs_sales_price").sum().alias("sum"))
            .sort(by="ca_zip", descending=False, nulls_last=False)
            .limit(100)
        )

    tpcds_q15(catalog_sales, customer, customer_address)
    return (tpcds_q15,)


@app.cell
def _():
    # q16
    return


@app.cell
def _():
    # q17
    return


@app.cell
def _():
    # q18
    return


@app.cell
def _():
    # q19
    return


@app.cell
def _():
    # q20
    return


if __name__ == "__main__":
    app.run()
