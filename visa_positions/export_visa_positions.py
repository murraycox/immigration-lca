__author__ = 'murray'
import json
import psycopg2


def get_connection():

    return psycopg2.connect("dbname='immigration-lca' user='murray' host='MCiMac.home' password='' connect_timeout=30")


def get_employers_for_industry(code):
    try:
        conn = get_connection()
        cur = conn.cursor()

    except:
        print("Problem connecting to the database")

    cur = conn.cursor()
    cur.execute("""
--Get Employers
SELECT
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "LCA_CASE_EMPLOYER_NAME" END AS employer,
                    SUM(applications)::integer AS applications,
                    SUM(positions)::integer AS positions,
                    AVG(lca_case_wage_rate_from)::integer AS wage_offer,
                    AVG(pw_1)::integer AS wage_prevailing
                        FROM
                            (SELECT

                                ROW_NUMBER() OVER (ORDER BY sum("TOTAL_WORKERS") DESC) AS row_number,
                                "LCA_CASE_EMPLOYER_NAME",
                                count(*) as applications,
                                sum("TOTAL_WORKERS") as positions,
                                AVG("LCA_CASE_WAGE_RATE_FROM") as lca_case_wage_rate_from,
                                AVG("PW_1") as pw_1
                            FROM h1_b
                                INNER JOIN naics_2012
                                    ON lca_case_naics_code = "NAICS_US_code"
                                INNER JOIN naics_2012 naics_2012_l2
                                    ON naics_2012.l2_parent = naics_2012_l2."NAICS_US_code"
                            WHERE
                                UPPER("STATUS") = 'CERTIFIED'
                                AND
                                "FULL_TIME_POS" = TRUE
                                AND
                                "LCA_CASE_WAGE_RATE_UNIT" = 'Year'
                                AND
                                "PW_UNIT_1" = 'Year'
                                AND
                                naics_2012_l2."NAICS_US_code" = '{0}'
                            GROUP BY
                                "LCA_CASE_EMPLOYER_NAME"
                            ORDER BY
                                positions desc) as h1_b_by_naics_l2
                GROUP BY
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "LCA_CASE_EMPLOYER_NAME" END
                ORDER BY MIN(row_number) ASC
                    """.format(code))

    employers = [];

    while True:
        row = cur.fetchone()
        if row is None:
            break
        (employer_name, applications_count, positions_count, wage_offer, wage_prevailing) = row

        employer = {"code": employer_name, "description": employer_name, "positionsCount": positions_count, "applicationsCount":applications_count, "wagePrevailing": wage_prevailing, "wageOffer": wage_offer, "type": "Employer", "positions": [] }
        positions = get_positions_for_industry_employer(code, employer_name)
        employer['positions'] = positions
        employers.append(employer)


    cur.close()
    conn.close()
    return employers



def get_positions_for_industry(code):
    #try:
    conn = get_connection()
    cur = conn.cursor()

    #except:
    #    print("Problem connecting to the database")

    cur = conn.cursor()
    cur.execute("""
                --Get positions
                SELECT
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc END AS sic_code,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc_title END AS sic_description,
                    SUM(applications)::INTEGER AS applications_count,
                    SUM(positions)::INTEGER AS positions_count,
                    ROUND(SUM(lca_case_wage_rate_from * positions)/SUM(positions))::INTEGER AS lca_case_wage_rate_from,
                    ROUND(SUM(pw_1 * positions)/SUM(positions))::INTEGER AS pw_1
                        FROM
                            (SELECT
                                ROW_NUMBER() OVER (ORDER BY sum("TOTAL_WORKERS") DESC) AS row_number,
                                soc_2010_parent.soc,
                                soc_2010_parent.soc_title,
                                COUNT(*) as applications,
                                SUM("TOTAL_WORKERS") as positions,
                                ROUND(AVG("LCA_CASE_WAGE_RATE_FROM")) as lca_case_wage_rate_from,
                                ROUND(AVG("PW_1")) as pw_1
                            FROM h1_b
                                INNER JOIN soc_2010
                                    ON "LCA_CASE_SOC_CODE" = soc
                                INNER JOIN soc_2010 soc_2010_parent
                                    ON soc_2010.l3_soc = soc_2010_parent.soc
                                INNER JOIN naics_2012
                                    ON "LCA_CASE_NAICS_CODE"::varchar = "NAICS_US_code"
                                INNER JOIN naics_2012 naics_2012_l2
                                    ON naics_2012.l2_parent = naics_2012_l2."NAICS_US_code"
                            WHERE
                                UPPER("STATUS") = 'CERTIFIED'
                                AND
                                "FULL_TIME_POS" = TRUE
                                AND
                                "LCA_CASE_WAGE_RATE_UNIT" = 'Year'
                                AND
                                "PW_UNIT_1" = 'Year'
                                AND
                                naics_2012_l2."NAICS_US_code" = '{0}'
                            GROUP BY
                                soc_2010_parent.soc,
                                soc_2010_parent.soc_title
                            ORDER BY
                                positions DESC) as h1_b_by_soc
                GROUP BY
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc END,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc_title END
                ORDER BY MIN(row_number) ASC
                    """.format(code))

    positions = [];

    while True:
        row = cur.fetchone()
        if row is None:
            break
        (sic_code, sic_description, applications_count, positions_count, wage_offer, wage_prevailing) = row
        position = {"sicCode": sic_code, "sicDescription": sic_description, "positionsCount": positions_count, "applicationsCount":applications_count, "wagePrevailing": wage_prevailing, "wageOffer": wage_offer }
        positions.append(position)


    cur.close()
    conn.close()
    return positions

def get_positions_for_industry_employer(code, employer):
    try:
        conn = get_connection()
        cur = conn.cursor()

    except:
        print("Problem connecting to the database")

    cur = conn.cursor()
    cur.execute("""
                --Get positions
                SELECT
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc END AS sic_code,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc_title END AS sic_description,
                    SUM(applications)::INTEGER AS applications_count,
                    SUM(positions)::INTEGER AS positions_count,
                    ROUND(SUM(lca_case_wage_rate_from * positions)/SUM(positions))::INTEGER AS lca_case_wage_rate_from,
                    ROUND(SUM(pw_1 * positions)/SUM(positions))::INTEGER AS pw_1
                        FROM
                            (SELECT
                                ROW_NUMBER() OVER (ORDER BY sum("TOTAL_WORKERS") DESC) AS row_number,
                                soc_2010_parent.soc,
                                soc_2010_parent.soc_title,
                                COUNT(*) as applications,
                                SUM("TOTAL_WORKERS") as positions,
                                ROUND(AVG("LCA_CASE_WAGE_RATE_FROM")) as lca_case_wage_rate_from,
                                ROUND(AVG("PW_1")) as pw_1
                            FROM h1_b
                                INNER JOIN soc_2010
                                    ON "LCA_CASE_SOC_CODE" = soc
                                INNER JOIN soc_2010 soc_2010_parent
                                    ON soc_2010.l3_soc = soc_2010_parent.soc
                                INNER JOIN naics_2012
                                    ON "LCA_CASE_NAICS_CODE"::varchar = "NAICS_US_code"
                                INNER JOIN naics_2012 naics_2012_l2
                                    ON naics_2012.l2_parent = naics_2012_l2."NAICS_US_code"
                            WHERE
                                UPPER("STATUS") = 'CERTIFIED'
                                AND
                                "FULL_TIME_POS" = TRUE
                                AND
                                "LCA_CASE_WAGE_RATE_UNIT" = 'Year'
                                AND
                                "PW_UNIT_1" = 'Year'
                                AND
                                naics_2012_l2."NAICS_US_code" = %s
                                AND
                                "LCA_CASE_EMPLOYER_NAME" = %s
                            GROUP BY
                                soc_2010_parent.soc,
                                soc_2010_parent.soc_title
                            ORDER BY
                                positions DESC) as h1_b_by_soc
                GROUP BY
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc END,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE soc_title END
                ORDER BY MIN(row_number) ASC
                    """, (code, employer,))

    positions = [];

    while True:
        row = cur.fetchone()
        if row is None:
            break
        (sic_code, sic_description, applications_count, positions_count, wage_offer, wage_prevailing) = row
        position = {"sicCode": sic_code, "sicDescription": sic_description, "positionsCount": positions_count, "applicationsCount":applications_count, "wagePrevailing": wage_prevailing, "wageOffer": wage_offer }
        positions.append(position)


    cur.close()
    conn.close()
    return positions

def get_industries():

    conn = get_connection()

    cur = conn.cursor()
    cur.execute("""
                SELECT
                        count(*) as applications,
                        sum("TOTAL_WORKERS") as positions,
                        AVG("LCA_CASE_WAGE_RATE_FROM")::integer as wage_offer,
                        AVG("PW_1")::integer as wage_prevailing
                    FROM h1_b
                    WHERE
                        UPPER("STATUS") = 'CERTIFIED'
                        AND
                        "FULL_TIME_POS" = TRUE
                        AND
                        "LCA_CASE_WAGE_RATE_UNIT" = 'Year'
                        AND
                        "PW_UNIT_1" = 'Year'
                    """)

    (applications, positions, wage_offer, wage_prevailing) = cur.fetchone()

    visaPositions = {"positionsCount": positions, "applicationsCount":applications, "wagePrevailing": wage_prevailing, "wageOffer": wage_offer, "maxWage": 220000, "type": "AllIndustries", "children": []}
    root = {"visaPositions": visaPositions}

    cur.close()
    cur = conn.cursor()

    cur.execute("""
                SELECT
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "NAICS_US_code" END AS code,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "NAICS_US_title" END AS description,
                    SUM(applications)::integer AS applications,
                    SUM(positions)::integer AS positions,
                    AVG(lca_case_wage_rate_from)::integer AS wage_offer,
                    AVG(pw_1)::integer AS wage_prevailing
                        FROM
                            (SELECT

                                ROW_NUMBER() OVER (ORDER BY sum("TOTAL_WORKERS") DESC) AS row_number,
                                naics_2012_l2."NAICS_US_code",
                                naics_2012_l2."NAICS_US_title",
                                count(*) as applications,
                                sum("TOTAL_WORKERS") as positions,
                                AVG("LCA_CASE_WAGE_RATE_FROM") as lca_case_wage_rate_from,
                                AVG("PW_1") as pw_1
                            FROM h1_b
                                INNER JOIN naics_2012
                                    ON lca_case_naics_code = "NAICS_US_code"
                                INNER JOIN naics_2012 naics_2012_l2
                                    ON naics_2012.l2_parent = naics_2012_l2."NAICS_US_code"
                            WHERE
                                UPPER("STATUS") = 'CERTIFIED'
                                AND
                                "FULL_TIME_POS" = TRUE
                                AND
                                "LCA_CASE_WAGE_RATE_UNIT" = 'Year'
                                AND
                                "PW_UNIT_1" = 'Year'
                            GROUP BY
                                naics_2012_l2."NAICS_US_code",
                                naics_2012_l2."NAICS_US_title"
                            ORDER BY
                                positions desc) as h1_b_by_naics_l2
                GROUP BY
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "NAICS_US_code" END,
                    CASE WHEN row_number > 10
                        THEN 'Others'
                        ELSE "NAICS_US_title" END
                ORDER BY MIN(row_number) ASC
                    """)


    while True:
        row = cur.fetchone()
        if row is None:
            break
        (code, description, applications, positions, wage_offer, wage_prevailing) = row
        industry = {"code": code, "description": description, "positionsCount": positions, "applicationsCount":applications, "wagePrevailing": wage_prevailing, "wageOffer": wage_offer, "type": "Industry", "positions": [], "children": [] }
        positions = get_positions_for_industry(code)
        industry['positions'] = positions
        employers = get_employers_for_industry(code)
        industry['children'] = employers
        visaPositions['children'].append(industry)


    cur.close()
    conn.close()

    print(json.dumps(root, indent=4))

get_industries()