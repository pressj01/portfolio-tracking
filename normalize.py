from config import get_connection


def populate_holdings():
    """Upsert from all_account_info into holdings."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.holdings AS target
        USING dbo.all_account_info AS source
        ON target.ticker = source.ticker
        WHEN MATCHED THEN UPDATE SET
            description = source.description,
            classification_type = source.classification_type,
            quantity = source.quantity,
            price_paid = source.price_paid,
            current_price = source.current_price,
            purchase_value = source.purchase_value,
            current_value = source.current_value,
            gain_or_loss = source.gain_or_loss,
            gain_or_loss_percentage = source.gain_or_loss_percentage,
            percent_change = source.percent_change
        WHEN NOT MATCHED THEN INSERT (
            ticker, description, classification_type, quantity,
            price_paid, current_price, purchase_value, current_value,
            gain_or_loss, gain_or_loss_percentage, percent_change
        ) VALUES (
            source.ticker, source.description, source.classification_type, source.quantity,
            source.price_paid, source.current_price, source.purchase_value, source.current_value,
            source.gain_or_loss, source.gain_or_loss_percentage, source.percent_change
        );
    """)
    row_count = cursor.rowcount
    conn.commit()
    conn.close()
    return row_count, f"Holdings populated: {row_count} rows affected."


def populate_dividends():
    """Upsert from all_account_info into dividends (includes new YTD/Total columns)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.dividends AS target
        USING dbo.all_account_info AS source
        ON target.ticker = source.ticker
        WHEN MATCHED THEN UPDATE SET
            div_frequency = source.div_frequency,
            reinvest = source.reinvest,
            ex_div_date = source.ex_div_date,
            div_per_share = source.div,
            dividend_paid = source.dividend_paid,
            estim_payment_per_year = source.estim_payment_per_year,
            approx_monthly_income = source.approx_monthly_income,
            annual_yield_on_cost = source.annual_yield_on_cost,
            current_annual_yield = source.current_annual_yield,
            ytd_divs = source.ytd_divs,
            total_divs_received = source.total_divs_received,
            paid_for_itself = source.paid_for_itself
        WHEN NOT MATCHED THEN INSERT (
            ticker, div_frequency, reinvest, ex_div_date, div_per_share,
            dividend_paid, estim_payment_per_year, approx_monthly_income,
            annual_yield_on_cost, current_annual_yield,
            ytd_divs, total_divs_received, paid_for_itself
        ) VALUES (
            source.ticker, source.div_frequency, source.reinvest, source.ex_div_date,
            source.div, source.dividend_paid, source.estim_payment_per_year,
            source.approx_monthly_income, source.annual_yield_on_cost,
            source.current_annual_yield,
            source.ytd_divs, source.total_divs_received, source.paid_for_itself
        );
    """)
    row_count = cursor.rowcount
    conn.commit()
    conn.close()
    return row_count, f"Dividends populated: {row_count} rows affected."


def populate_income_tracking():
    """Append snapshot to income_tracking (skip duplicates by ticker+import_date)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dbo.income_tracking (
            ticker, import_date, dividend_paid,
            approx_monthly_income, estim_payment_per_year,
            dollars_per_hour, ytd_divs, total_divs_received
        )
        SELECT
            a.ticker, a.import_date, a.dividend_paid,
            a.approx_monthly_income, a.estim_payment_per_year,
            a.dollars_per_hour, a.ytd_divs, a.total_divs_received
        FROM dbo.all_account_info a
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.income_tracking i
            WHERE i.ticker = a.ticker AND i.import_date = a.import_date
        )
    """)
    row_count = cursor.rowcount
    conn.commit()
    conn.close()
    return row_count, f"Income tracking: {row_count} rows inserted."


def populate_pillar_weights():
    """Upsert from all_account_info into pillar_weights (only if pillar columns are populated)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.pillar_weights AS target
        USING (
            SELECT ticker, hedged_anchor, anchor, gold_silver,
                   booster, juicer, bdc, growth
            FROM dbo.all_account_info
            WHERE hedged_anchor IS NOT NULL OR anchor IS NOT NULL
               OR gold_silver IS NOT NULL OR booster IS NOT NULL
               OR juicer IS NOT NULL OR bdc IS NOT NULL OR growth IS NOT NULL
        ) AS source
        ON target.ticker = source.ticker
        WHEN MATCHED THEN UPDATE SET
            hedged_anchor = source.hedged_anchor,
            anchor = source.anchor,
            gold_silver = source.gold_silver,
            booster = source.booster,
            juicer = source.juicer,
            bdc = source.bdc,
            growth = source.growth
        WHEN NOT MATCHED THEN INSERT (
            ticker, hedged_anchor, anchor, gold_silver,
            booster, juicer, bdc, growth
        ) VALUES (
            source.ticker, source.hedged_anchor, source.anchor, source.gold_silver,
            source.booster, source.juicer, source.bdc, source.growth
        );
    """)
    row_count = cursor.rowcount
    conn.commit()
    conn.close()
    return row_count, f"Pillar weights populated: {row_count} rows affected."
