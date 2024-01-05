from utils import config, database_service
import pandas as pd
from numpy import NINF, inf
from apiclient import discovery
import re
from google.oauth2 import service_account
import boto3
import datetime
import joblib
import numpy as np
import snowflake.connector
import logging
import warnings
import json

warnings.filterwarnings("ignore")
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.setLevel('INFO')
from utils.error import ModelError, ErrorLevel
mod_err = ModelError()

region_name = "ap-southeast-1"
runtime_client = boto3.client(service_name='sagemaker-runtime', region_name=region_name)


prod_cfg = config.get_config()['prod_db']
productionDBLoan = database_service.DataService(
    host=prod_cfg['host'],
    username=prod_cfg['username'],
    password=prod_cfg['password'],
    port=prod_cfg['port'],
    database='loan_db')

# productionDBMember = database_service.DataService(
#     host=prod_cfg['host'],
#     username=prod_cfg['username'],
#     password=prod_cfg['password'],
#     port=prod_cfg['port'],
#     database='member_db')

prod_cfg_id = config.get_config()['prod_db_id']
productionDBLoan_id = database_service.DataService(
    host=prod_cfg_id['host'],
    username=prod_cfg_id['username'],
    password=prod_cfg_id['password'],
    port=prod_cfg_id['port'],
    database='loan_db')

# datalake_cfg = config.get_config()['datalake_write']
# datalakeDB = database_service.DataService(
#     host=datalake_cfg['host'],
#     username=datalake_cfg['username'],
#     password=datalake_cfg['password'],
#     port=datalake_cfg['port'],
#     database='datalake')


snowflake_cfg = config.get_config()['snowflake']

# productionDBDocsParsing = database_service.DataService(
#     host=prod_cfg['host'],
#     username=prod_cfg['username'],
#     password=prod_cfg['password'],
#     port=prod_cfg['port'],
#     database='docs_parsing_db')


# rds_cfg = config.get_config()['data_rds_prod']
# productionRDSResults = database_service.DataService(
#     host=rds_cfg['host'],
#     username=rds_cfg['username'],
#     password=rds_cfg['password'],
#     port=rds_cfg['port'],
#     database=rds_cfg['database'])


class horus:

    def __init__(self, othParams, facility_code):

        self.facility_code = facility_code
        # self.industryRiskMap = industryRiskMap
        # self.riskQuantumTenorMap = riskQuantumTenorMap
        self.othParams = othParams
        self.all_bs_data = pd.DataFrame()
        # self.avg_non_trade_trans = self.get_credit_v2_nonTradeIncome()
        # NOTE TO DEV: Pull from GS --done
        try:
            self.bankStatements, self.pefindo_input, self.borrowerID_gs, self.platform_code, self.product_name = self.fetch_model_inputs()
            logger.info(f"\nBank Statements \n{self.bankStatements.iloc[0]}")
            logger.info(f"\nPefindo Input \n{self.pefindo_input.iloc[0]}")
            # print("Bank Statements\n", self.bankStatements.iloc[0], '\n')
            # print("Pefindo Input\n", self.pefindo_input.iloc[0], '\n')
            # print("Industry Sector: ", self.industry_sector)
        except Exception as e:
            mod_err.raise_exception(str(e), ErrorLevel.WARNING,
                                    data=f'Failed to get the Bank statements and Pefindo data for the facility code: {self.facility_code}')
            self.bankStatements = pd.DataFrame()
            self.bankStatements['bankEndBalances'] = np.NaN
            self.bankStatements['bankLiabilities'] = np.NaN
            self.bankStatements['bankCredit'] = np.NaN
            self.bankStatements['bankDebit'] = np.NaN
            self.bankStatements['bankEndBalancesPct'] = np.NaN
            self.bankStatements['bankLiabilitiesPct'] = np.NaN
            self.bankStatements['bankCreditPct'] = np.NaN
            self.bankStatements['bankDebitPct'] = np.NaN
            self.bankStatements['DCRatio'] = np.NaN
            self.bankStatements['balanceCreditRatio'] = np.NaN
            self.bankStatements['balanceDebitRatio'] = np.NaN
            self.bankStatements['balanceLiabilityRatio'] = np.NaN
            self.bankStatements['DCRatioPct'] = np.NaN
            self.bankStatements['balanceCreditRatioPct'] = np.NaN
            self.bankStatements['balanceDebitRatioPct'] = np.NaN
            self.bankStatements['balanceLiabilityRatioPct'] = np.NaN
            self.bankStatements['bankStatementMonths'] = 0
            self.pefindo_input = pd.DataFrame()

        # self.applicationDate = self.get_application_date()
        # print('self.applicationDate', self.applicationDate)
        # self.applicationYrMth = self.get_application_yrmth()
        # self.loanQT = self.get_loan_application_quantum_tenor()
        #if len(str(self.loanID)) == 0 or int(self.loanID) is None or math.isnan(int(self.loanID)):
        #self.loanID = self.get_loan_id()
        #if not (int(self.loanID) is None) and not (math.isnan(int(self.loanID))) and len(str(self.loanID)) != 0:
        self.borrowerID = self.borrowerID_gs #self.get_borrower_id()
        self.historicalDPD = self.get_historical_dpd()
        # self.loan_code = self.get_loan_code()
        # self.riskQuantum = None
        # self.riskTenor = None
        self.applicationData = self.invoke()
        self.loan_date = self.get_loan_date()
        self.quantumCap = None
        self.tenorCap = None
        self.modelInput = None
        self.min_quantum = 3000  # Minimum quantum for bolt product
        # Note: Not hardcoded, fetching them from gsheet. It is just initialization.
        self.ead = .60
        self.lgd = 1
        self.int_floor = None
        self.int_limit = None
        self.Application_Score = None
        self.Behavioural_Data = [None, None, None, None, None, None]
        self.Behavioural_Data_without_changes = [None, None, None, None, None, None]
        self.ead = self.get_oth_params('EAD')
        self.lgd = self.get_oth_params('LGD')
        self.int_floor = self.get_oth_params('int_floor')
        self.int_limit = self.get_oth_params('int_limit')

        logger.info(f"ead {self.ead}")
        logger.info(f"lgd {self.lgd}")

    def google_sheet_to_dataframe(self, sheet_id, sheet_name, sheet_range=''):
        scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(
            'config/client_secret.json', scopes=scope)
        SERVICE_NAME = 'sheets'
        API_VERSION = 'v4'
        SERVICE = discovery.build(SERVICE_NAME, API_VERSION, credentials=credentials)
        read_range = ("!".join([sheet_name, sheet_range]))
        result = SERVICE.spreadsheets().values().get(spreadsheetId=sheet_id, range=read_range).execute()
        rows = result['values']
        column_names = rows[0]
        m = re.compile('[^\w\d]+')
        column_names = [re.sub(m, '_', i).strip().upper() for i in column_names]
        df = pd.DataFrame(rows[1:], columns=column_names)
        df = df.iloc[::-1]
        return df[df['FACILITY_CODE'] == str(self.facility_code)]#.head(1)

    def fetch_model_inputs(self):
        sheet_id = '1NcE4mXzJWqcm9V3GfiIH03-qPhxtpM23mmV4Ge2YI6g'
        sheet_name = 'Model Port (ID line)'
        sheet_range = 'B3:AD2000'
        df = self.google_sheet_to_dataframe(sheet_id, sheet_name, sheet_range)
        #print(df.iloc[0].to_string())
        # Separating both the Inputs
        df_bank = df.loc[:, "AVGBANKMONTHENDBALANCES":]
        df_pefindo = df.loc[:, :"LAINNYA"]
        borrower_id = df.loc[:, "BORROWER_ID":"BORROWER_ID"].values[0][0]
        platform_code = df.loc[:, "PLATFORM_CODE":"PLATFORM_CODE"].values[0][0]
        product = df.loc[:, "PRODUCT":"PRODUCT"].values[0][0]
        df_bank = df_bank.reset_index(drop=True)
        df_pefindo = df_pefindo.reset_index(drop=True)
        self.all_bs_data = df_bank
        # print("Fetched model inputs before transformations:\n")
        # print(df_bank.to_string())
        # print("\n")
        # print(df_pefindo.to_string())
        # print("\n\n")
        # print("loan_id",loan_id)
        return self.transform_bs(df_bank), self.transform_pefindo(df_pefindo), borrower_id, platform_code, product

    @staticmethod
    def transform_bs(df_bank):
        df_bank_transform = pd.DataFrame(columns=["monthly_end_balance_pct",
                                                  "monthly_debit_pct",
                                                  "bal_credit_ratio_pct",
                                                  "dc_ratio_pct",
                                                  "monthly_credit_pct",
                                                  "bal_debit_ratio_pct"
                                                  ])

        df_bank_transform["bal_credit_ratio_pct"] = df_bank["BALANCECREDITRATIOPCT"]
        df_bank_transform["monthly_debit_pct"] = df_bank["BANKDEBITPCT"]
        df_bank_transform["dc_ratio_pct"] = df_bank["DCRATIOPCT"]
        df_bank_transform["monthly_credit_pct"] = df_bank["BANKCREDITPCT"]
        df_bank_transform["monthly_end_balance_pct"] = df_bank["BANKENDBALANCESPCT"]
        df_bank_transform["bal_debit_ratio_pct"] = df_bank["BALANCEDEBITRATIOPCT"]
        # df_bank_transform["monthly_credit"] = float(df_bank["BANKCREDIT"])

        df_bank_transform = df_bank_transform.replace(np.inf, 333333333333)
        df_bank_transform = df_bank_transform.replace(-np.inf, -444444444444)
        df_bank_transform = df_bank_transform.replace('#N/A', np.nan)
        df_bank_transform = df_bank_transform.replace('None', np.nan)
        df_bank_transform = df_bank_transform.fillna(999999999999)
        return df_bank_transform

    @staticmethod
    def transform_pefindo(df_pefindo):
        df_pefindo_transform = pd.DataFrame(columns=["idscore",
                                                     "fasilitas_aktif_jenis_fasilitas_bg_diterbitkan",
                                                     "maks._usia_tunggakan",
                                                     "fasilitas_aktif_jenis_fasilitas_lainnya"])

        df_pefindo_transform["idscore"] = df_pefindo["IDSCORE"]  #
        df_pefindo_transform["fasilitas_aktif_jenis_fasilitas_bg_diterbitkan"] = df_pefindo["BG_DITERBITKAN"]  #
        df_pefindo_transform["fasilitas_aktif_jenis_fasilitas_lainnya"] = df_pefindo["LAINNYA"]  #
        df_pefindo_transform["maks._usia_tunggakan"] = df_pefindo["MAKS__USIA_TUNGGAKAN"]  #

        df_pefindo_transform = df_pefindo_transform.replace(np.inf, 333333333333)
        df_pefindo_transform = df_pefindo_transform.replace(-np.inf, -444444444444)
        df_pefindo_transform = df_pefindo_transform.replace('#N/A', np.nan)
        df_pefindo_transform = df_pefindo_transform.replace('None', np.nan)
        df_pefindo_transform = df_pefindo_transform.fillna(999999999999)
        return df_pefindo_transform

    @staticmethod
    def predict_bs(feature_list_bs):
        content_type = "application/json"
        request_body = {"Input": [feature_list_bs]}
        data = json.loads(json.dumps(request_body))
        payload = json.dumps(data)
        endpoint_name = "id-bank-statements-ep-2022-12-22"
        response = runtime_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType=content_type,
            Body=payload
        )
        result = json.loads(response['Body'].read().decode())['Output']
        return result

    @staticmethod
    def predict_pefindo(feature_list_ctos):
        content_type = "application/json"
        request_body = {"Input": [feature_list_ctos]}
        data = json.loads(json.dumps(request_body))
        payload = json.dumps(data)
        endpoint_name = "id-pefindo-statements-ep-2022-12-22"
        response = runtime_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType=content_type,
            Body=payload
        )
        result = json.loads(response['Body'].read().decode())['Output']
        return result

    def check_values(self):
        print(self.borrowerID)
        print(self.borrowerUEN)
        print(self.applicationData)

    # def get_application_date(self):
    #
    #     application_date = productionDBLoan.query_postgres('''
    #         SELECT
    #             LEFT(created_at::TEXT, 10) AS application_date
    #         FROM loans
    #         WHERE id=%(loanID)s;
    #     ''', params={'loanID': self.loanID})['application_date'].values[0]
    #
    #     return application_date
    #
    # def get_application_yrmth(self):
    #
    #     application_yrmth = productionDBLoan.query_postgres('''
    #         SELECT
    #             -- LEFT(created_at::TEXT, 7) AS application_yrmth
    #             LEFT(updated_at::TEXT, 7) AS application_yrmth
    #         FROM loans
    #         WHERE id=%(loanID)s;
    #     ''', params={'loanID': self.loanID})['application_yrmth'].values[0]
    #
    #     return application_yrmth

    # # Check with DE if we have this in prod bd, otherwise take from GS
    # def get_loan_application_quantum_tenor(self):
    #
    #     df = productionDBLoan.query_postgres('''
    #         SELECT
    #             applied_amount,
    #             applied_tenor,
    #             tenor_type
    #         FROM loans
    #         WHERE id=%(loanID)s
    #     ''', params={'loanID': self.loanID})
    #
    #     # print("Tenor table:\n", df.to_string())
    #     df['applied_tenor'] = np.where(df['tenor_type'] == 1, df['applied_tenor'] / 30, df['applied_tenor'])
    #
    #     qt = {
    #         'appliedQuantum': df['applied_amount'].values[0],
    #         'appliedTenor': df['applied_tenor'].values[0]
    #     }
    #
    #     return qt

    def get_historical_dpd(self):

        if str(self.platform_code).upper() == "ZI" or str(self.platform_code).upper() == "ZU":
            country_code = 'SG'
        else:
            country_code = 'ID'

        with snowflake.connector.connect(
                user=snowflake_cfg['username'],
                password=snowflake_cfg['password'],
                account=snowflake_cfg['host']
        ) as conn:

            conn.cursor().execute('USE WAREHOUSE DATA_SCIENCE');

            historical_dpd = pd.read_sql('''
                select LOAN_CODE,LOAN_MAX_DPD_LCL_DAYS from ADM.TRANSACTION.LOAN_DENORM_T 
                where CMD_CTR_BORROWER_ID = %(borrower_id)s
                and LOAN_RDS_COUNTRY_CODE = %(county_code)s
                and loan_stage_id = 5 
                
                ''', params={'borrower_id': self.borrowerID, 'county_code':country_code},
                                         con=conn)
        historical_dpd.columns = [str.lower(col) for col in
                                  historical_dpd.columns]
        if historical_dpd.shape[0] == 0:
            return None
        else:
            return historical_dpd['loan_max_dpd_lcl_days'].max()

    def get_oth_params(self, item, format_numeric=True):
        """Helper function for getting params from google sheet
        Use format_numeric if numbers are required"""
        # print(self.othParams.to_string())
        sheet_item = self.othParams['ITEM'].str.strip().str.lower()
        item = item.strip().lower()
        assert self.othParams.loc[sheet_item == item].shape[0] == 1
        result = self.othParams.loc[sheet_item == item].values[0][1]
        # print(result)
        # Changes to handle new EL_limit param
        if str(result).lower() == 'na':
            return None
        else:
            if format_numeric:
                result = float(result.replace(',', ''))
            return result

    def get_loan_date(self):

        if str(self.platform_code).upper() == "ZI" or str(self.platform_code).upper() == "ZU":
            DBLoan = productionDBLoan
        else:
            DBLoan = productionDBLoan_id
        try:
            if str(self.product_name).lower() != "tl enterprise":
                loan_date = DBLoan.query_postgres('''
                            SELECT max(created_at) as created_date
                            FROM loans
                            WHERE facility_code=%(facility_code)s;
                        ''', params={'facility_code': self.facility_code})['created_date'].values[0]
            else:
                loan_date = DBLoan.query_postgres('''
                                            SELECT max(created_at) as created_date
                                            FROM loans
                                            WHERE loan_code=%(facility_code)s;
                                        ''', params={'facility_code': self.facility_code})['created_date'].values[0]


            #print("Borrower ID", borrower_id)
        except:
            #print("Borrower ID from the Gsheet")
            loan_date = pd.to_datetime("today", utc=True)

        return loan_date

    # def get_loan_id(self):
    #     try:
    #         loan_id = productionDBLoan.query_postgres('''
    #                     SELECT id
    #                     FROM loans
    #                     WHERE facility_code=%(facility_code)s;
    #                 ''', params={'facility_code': self.facility_code})['id'].values[0]
    #     except:
    #         loan_id = "NA"
    #
    #     # if len(str(loan_id)) == 0 or int(loan_id) is None or math.isnan(int(loan_id)):
    #     #     loan_id = "NA"
    #     return loan_id
    #
    # def get_loan_code(self):
    #     loan_code = productionDBLoan.query_postgres('''
    #           select loan_code from loans
    #            WHERE id=%(loanID)s;
    #        ''', params={'loanID': self.loanID})['loan_code'].values[0]
    #     return loan_code

    def invoke(self):
        data = pd.DataFrame({
            'borrowerID': [self.borrowerID],
        })
        # for key, value in self.loanQT.items():
        #     data[key] = value
        data = pd.concat([data, self.bankStatements, self.pefindo_input], axis=1, join='inner')

        return data


    def prophesize(self):
        data = self.applicationData
        logger.info(f"Data inside prophesize: \n{data.iloc[0]}")
        # print("Data inside prophesize:\n", data.iloc[0], "\n")
        results = {}

        # results['applied_quantum'] = data['appliedQuantum'].values[0]
        # results['applied_tenor'] = data['appliedTenor'].values[0]
        # logger.info(f"applied quantum: {data['appliedQuantum'].values[0]}")
        # logger.info(f"applied tenor: {data['appliedTenor'].values[0]}")

        woeMaps = get_gsheet_values('1NcE4mXzJWqcm9V3GfiIH03-qPhxtpM23mmV4Ge2YI6g', 'WOE Table', "A1:E100")
        # print(woeMaps.head().to_string())
        woeMaps = woeMaps.reset_index(drop=True)
        woeMaps = woeMaps.replace('-444444444444', NINF)
        woeMaps = woeMaps.replace('333333333333', inf)

        woe_cols_bs = ["monthly_end_balance_pct",
                       "monthly_debit_pct",
                       "bal_credit_ratio_pct",
                       "dc_ratio_pct",
                       "monthly_credit_pct",
                       "bal_debit_ratio_pct"]

        woe_cols_pefindo = ["idscore",
                            "fasilitas_aktif_jenis_fasilitas_bg_diterbitkan",
                            "maks._usia_tunggakan",
                            "fasilitas_aktif_jenis_fasilitas_lainnya"]

        def woeMapFN(data, woe_cols):
            """
            Map WOE values to feature bins.
            @param data: pandas.DataFrame: Feature set and woe_cols
            @return: pandas.DataFrame: Feature set replaced with WOE values
            """
            for col in woe_cols:
                data[col] = float(data[col])
                # print("col:", col)
                colValues = woeMaps[woeMaps['VARIABLE'].eq(col)]
                # print(colValues)
                condition = '[' + \
                            ','.join(colValues.apply(
                                lambda x: f"(data[col]>={(x['BIN_START'])}) & (data[col]<{(x['BIN_END'])})",
                                axis=1).values) + \
                            ']'
                # print("condition:", condition)
                values = colValues['WOE_VALUES']
                # print("values:", values)
                data[f'woe_{col}'] = np.select(eval(condition), values, default=np.nan)
            return data

        features_bs = data[woe_cols_bs]
        features_pefindo = data[woe_cols_pefindo]
        # print(data.to_string())
        # print(features_bs.to_string())
        # print(features_pefindo.to_string())
        feature_list_bs = woeMapFN(features_bs, woe_cols_bs)
        feature_list_pefindo = woeMapFN(features_pefindo, woe_cols_pefindo)
        # data = woeMapFN(data)

        feat_bs = [
            "woe_monthly_end_balance_pct",
            "woe_monthly_debit_pct",
            "woe_bal_credit_ratio_pct",
            "woe_dc_ratio_pct",
            "woe_monthly_credit_pct",
            "woe_bal_debit_ratio_pct"
        ]

        feat_pefindo = ["woe_idscore",
                     "woe_fasilitas_aktif_jenis_fasilitas_bg_diterbitkan",
                     "woe_maks._usia_tunggakan",
                     "woe_fasilitas_aktif_jenis_fasilitas_lainnya"
                     ]
        feature_list_bs = feature_list_bs[feat_bs]
        feature_list_pefindo = feature_list_pefindo[feat_pefindo]
        feature_list_pefindo.rename(columns={'woe_fasilitas_aktif_jenis_fasilitas_bg_diterbitkan': 'fasilitas_aktif_jenis_fasilitas_bg_diterbitkan_woe'
                                          , 'woe_fasilitas_aktif_jenis_fasilitas_lainnya': 'fasilitas_aktif_jenis_fasilitas_lainnya_woe'
                                          , 'woe_idscore': 'idscore_woe'
                                          , 'woe_maks._usia_tunggakan': 'maks._usia_tunggakan_woe'}, inplace=True)


        # # For Deployment
        feature_list_bs = list(feature_list_bs.values[0])
        feature_list_pefindo = list(feature_list_pefindo.values[0])
        feature_list_bs = (np.array(feature_list_bs).astype(np.float64)).tolist()
        feature_list_pefindo = (np.array(feature_list_pefindo).astype(np.float64)).tolist()

        default_prob1 = self.predict_bs(feature_list_bs)
        default_prob2 = self.predict_pefindo(feature_list_pefindo)

        # For Local testing
        # with open('id_line_model_bank.joblib', 'rb') as f:
        #     predictor = joblib.load(f)
        #     default_prob1 = predictor.predict_proba(feature_list_bs)[0][1]
        #
        # with open('id_line_model_pefindo.joblib', 'rb') as f:
        #     predictor = joblib.load(f)
        #     default_prob2 = predictor.predict_proba(feature_list_pefindo)[0][1]


        default_prob = (0.3 * default_prob1 + 0.7 * default_prob2)
        logger.info(f"Probability BS: {default_prob1}")
        logger.info(f"Probability Pefindo: {default_prob2}")
        logger.info(f"Average Probability of default: {default_prob}")

        results['BS_PD'] = default_prob1
        results['Pefindo_PD'] = default_prob2
        results['FinalPD'] = default_prob
        return results


    #getting behavioural_data
    def get_behavioural_data(self):
        with snowflake.connector.connect(
                user=snowflake_cfg['username'],
                password=snowflake_cfg['password'],
                account=snowflake_cfg['host']
        ) as conn:
            conn.cursor().execute('USE WAREHOUSE DATA_SCIENCE');
            
            # Payment Behaviour - Percentage Full payments
            percentage_full_payments_df = pd.read_sql("""SELECT CMD_CTR_BORROWER_ID, 
            SUM(CASE WHEN INSTALLMENT_PAYMENT_STATUS_CODE='PAID' THEN 1 ELSE 0 END)*100/COUNT(*) AS PERCENTAGE_FULL_PAYMENTS_LAST_YEAR
            FROM ADM.TRANSACTION.INSTALLMENT_DENORM_T
            WHERE CMD_CTR_BORROWER_ID = %(borrower_id)s
                AND LOAN_STAGE_ID = 5
                AND LOAN_PLATFORM_CODE IN ('ID','ZI')
                AND CMD_CTR_ADJUSTED_PRODUCT_ID IN (13,38,32,35,16,18,34,17,-102,-104,60)
                AND INSTALLMENT_PAYMENT_DEADLINE_LCL_TS<GETDATE()
                AND INSTALLMENT_PAYMENT_DEADLINE_LCL_TS>DATEADD(YEAR, -1, GETDATE())
            GROUP BY CMD_CTR_BORROWER_ID;""", params = {'borrower_id':self.borrowerID}, con=conn) 
            
            # DPD Behaviour - Percentage DPD0
            percentage_dpd_df = pd.read_sql("""SELECT CMD_CTR_BORROWER_ID, 
            SUM(CASE WHEN LOAN_MAX_DPD_LCL_DAYS>0 THEN 1 ELSE 0 END)*100/COUNT(*) AS PERCENTAGE_DPD
            FROM ADM.TRANSACTION.LOAN_DENORM_T
            WHERE CMD_CTR_BORROWER_ID = %(borrower_id)s
                AND LOAN_STAGE_ID = 5
                AND LOAN_PLATFORM_CODE IN ('ID','ZI')
                AND CMD_CTR_ADJUSTED_PRODUCT_ID IN (13,38,32,35,16,18,34,17,-102,-104,60)
            GROUP BY CMD_CTR_BORROWER_ID;""", params = {'borrower_id':self.borrowerID}, con=conn) 
            
            # Historical Loan and Facility Information - tenure and number of applied loans
            tenure_df = pd.read_sql("""SELECT CMD_CTR_BORROWER_ID,
            DATEDIFF(DAY, MIN(LOAN_CREATED_AT_LCL_TS), GETDATE()) AS TENURE
            FROM ADM.TRANSACTION.LOAN_DENORM_T
            WHERE CMD_CTR_BORROWER_ID = %(borrower_id)s
                AND LOAN_STAGE_ID = 5
                AND LOAN_PLATFORM_CODE IN ('ID','ZI')
                AND CMD_CTR_ADJUSTED_PRODUCT_ID IN (13,38,32,35,16,18,34,17,-102,-104,60)
            GROUP BY CMD_CTR_BORROWER_ID;""", params = {'borrower_id':self.borrowerID}, con=conn) 
            
            number_of_applied_loans_df = pd.read_sql("""SELECT CMD_CTR_BORROWER_ID,
            count(*) AS NUMBER_OF_APPLIED_LOANS_LAST_YEAR
            FROM ADM.TRANSACTION.LOAN_DENORM_T
            WHERE CMD_CTR_BORROWER_ID = %(borrower_id)s
                AND LOAN_PLATFORM_CODE IN ('ID','ZI')
                AND LOAN_CREATED_AT_LCL_TS<GETDATE()
                AND LOAN_CREATED_AT_LCL_TS>DATEADD(YEAR, -1, GETDATE())
                AND CMD_CTR_ADJUSTED_PRODUCT_ID IN (13,38,32,35,16,18,34,17,-102,-104,60)
            GROUP BY CMD_CTR_BORROWER_ID;""", params = {'borrower_id':self.borrowerID}, con=conn) 
            
            # Application Score - application model score from one year ago
            application_pd_one_year_before_df = pd.read_sql("""SELECT CMD_CTR_BORROWER_ID, 
            LOAN_CREATED_DATE, APPLICATION_PD
            FROM CBM.DATA_SCIENCE_MODEL_RESULTS.ID_LINE_HORUS_PRIME
            WHERE CMD_CTR_BORROWER_ID = %(borrower_id)s and SCORING_TIMESTAMP IS NOT NULL;""", params = {'borrower_id':self.borrowerID}, con=conn)
        
        percentage_full_payments_df.columns = [str.lower(col) for col in percentage_full_payments_df.columns]
        percentage_dpd_df.columns = [str.lower(col) for col in percentage_dpd_df.columns]
        tenure_df.columns = [str.lower(col) for col in tenure_df.columns]
        number_of_applied_loans_df.columns = [str.lower(col) for col in number_of_applied_loans_df.columns]
        application_pd_one_year_before_df.columns = [str.lower(col) for col in application_pd_one_year_before_df.columns]
        
        # Dealing with None values
        if percentage_full_payments_df.shape[0]==0:
            percentage_full_payments = None
        else:
            percentage_full_payments = percentage_full_payments_df['percentage_full_payments_last_year'].mean()
        if percentage_dpd_df.shape[0]==0:
            percentage_dpd = None
        else:
            percentage_dpd = percentage_dpd_df['percentage_dpd'].mean()
        if tenure_df.shape[0]==0:
            tenure = None
        else:
            tenure = tenure_df['tenure'].mean()
        if number_of_applied_loans_df.shape[0]==0:
            number_of_applied_loans_last_year = None
        else:
            number_of_applied_loans_last_year = number_of_applied_loans_df['number_of_applied_loans_last_year'].mean()
        
        # getting application PD from one year ago
        if application_pd_one_year_before_df.shape[0]==0:
            application_score_pd_one_year_before = None
        else:
            application_pd_one_year_before_df['loan_created_date'] = pd.to_datetime(application_pd_one_year_before_df['loan_created_date'], utc=True)
            one_year_from_today = pd.to_datetime('today', utc=True) - pd.DateOffset(years=1)
            result = application_pd_one_year_before_df.loc[application_pd_one_year_before_df['loan_created_date'].sub(one_year_from_today).abs().idxmin()]
            application_score_pd_one_year_before = result['application_pd'].mean()
        
        l = []
        l.append([percentage_full_payments,percentage_dpd,tenure, number_of_applied_loans_last_year, application_score_pd_one_year_before])
        behaviour_data = pd.DataFrame(l,  columns = ['percentage_full_payments','percentage_dpd','tenure','number_of_applied_loans_last_year','application_score_pd_one_year_before'])
        
        #final behavioural data
        self.Behavioural_Data = behaviour_data
        self.Behavioural_Data_without_changes = behaviour_data
        return behaviour_data


    def behavioural_prophesize(self):
        #b_data['application_score'] = output['FinalPD'].mean()
        b_data = self.Behavioural_Data
        app_score = self.Application_Score
        b_data['application_score'] = app_score
        # if pd from one year ago is not available, then change in pd is 0
        b_data['application_score_pd_one_year_before'] = b_data['application_score_pd_one_year_before'].fillna(app_score)
        # calculating change in PD
        b_data['change_in_pd'] = (b_data['application_score'] - b_data['application_score_pd_one_year_before'])*100/(b_data['application_score_pd_one_year_before']+0.0001)
        
        def transform_behavioural(df_behavioural):
            df_behavioural = df_behavioural[['percentage_full_payments', 'percentage_dpd', 'tenure',
               'number_of_applied_loans_last_year', 'application_score',
               'change_in_pd']]
            df_behavioural = df_behavioural.replace(np.inf, 333333333333)
            df_behavioural = df_behavioural.replace(-np.inf, -444444444444)
            df_behavioural = df_behavioural.replace('#N/A', np.nan)
            df_behavioural = df_behavioural.replace('None', np.nan)
            df_behavioural = df_behavioural.fillna(999999999999)
            return df_behavioural
        
        b_data = transform_behavioural(b_data)
        
        # WOE Binning
        woeMaps = get_gsheet_values('1FyTx8Uw2OxBajGmRKGxB-1O21y5nuvzzTIClmJhQx6g', 'WOE Table - Behavioural', "A1:E100")
        woeMaps = woeMaps.reset_index(drop=True)
        woeMaps['BIN_START'] = woeMaps['BIN_START'].astype(float)
        woeMaps['BIN_END'] = woeMaps['BIN_END'].astype(float)
        cols = ['%_of_full_payments_last_year', '%_dpd',
           'date_difference_from_earliest_loan',
           'number_of_applied_loans_lastyear', 'final_pred_v1', 'change_in_pd']
        replacement_dict = {'percentage_full_payments':'%_of_full_payments_last_year', 'application_score':'final_pred_v1', 'percentage_dpd':'%_dpd','tenure':'date_difference_from_earliest_loan','number_of_applied_loans_last_year':'number_of_applied_loans_lastyear'}
        b_data.columns = [replacement_dict.get(col, col) for col in b_data.columns]
        def apply_binning(value, binning_map, column):
            binning_map = binning_map[binning_map['VARIABLE']==column]
            for index, row in binning_map.iterrows():
                if row['BIN_START'] <= value < row['BIN_END']:
                    return row['WOE']
            return None
        for column in cols:
            b_data[column+'_woe'] = b_data[column].apply(lambda x: apply_binning(x, woeMaps, column))
        beh_input_data = b_data[[
           'date_difference_from_earliest_loan_woe','%_dpd_woe','final_pred_v1_woe','change_in_pd_woe','%_of_full_payments_last_year_woe',
           'number_of_applied_loans_lastyear_woe']]

        # need to change this part before deployment
        # change needed
        #feature_list_behavioural = list(beh_input_data.values[0])
        #feature_list_behavioural = (np.array(feature_list_behavioural).astype(np.float64))
        with open('id_line_behavioural_model_file.joblib', 'rb') as f:
            beh_predictor = joblib.load(f)
            beh_def_prob = beh_predictor.predict_proba(beh_input_data)[0][1]
        logger.info(f"Behavioural Features: {self.Behavioural_Data}")
        logger.info(f"Probability Behavioural: {beh_def_prob}")
        return b_data, beh_def_prob

def write_to_snowflake(df):
    """
        Writes a pandas DataFrame to a Snowflake database.
        Args:
            df (pandas.DataFrame): The DataFrame to be written to the Snowflake table.
        Raises:
            snowflake.connector.errors.Error: If an error occurs while executing the SQL statements.
        Returns:
            None
        """
    try:
        with snowflake.connector.connect(
                user=snowflake_cfg['username'],
                password=snowflake_cfg['password'],
                account=snowflake_cfg['host'],
                database='CBM',
                schema='DATA_SCIENCE_MODEL_RESULTS'
        ) as conn:

            cur = conn.cursor().execute('USE WAREHOUSE DATA_SCIENCE');
            # Each tuple represents a row in the DataFrame
            values_to_log = [tuple(x) for x in df.values]
            # print(values_to_log)
            table_name = 'ID_LINE_HORUS_PRIME'
            columns = ','.join(df.columns)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({','.join(['%s'] * len(df.columns))})"
            # Can handle insertion od multiple rows
            cur.executemany(query, values_to_log)
            conn.commit()

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

def get_gsheet_values(sheet_id, sheet_name, sheet_range=''):
    scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    credentials = service_account.Credentials.from_service_account_file(
        'config/client_secret.json', scopes=scope)
    SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SERVICE = discovery.build(SERVICE_NAME, API_VERSION, credentials=credentials)
    read_range = ("!".join([sheet_name, sheet_range]))
    result = SERVICE.spreadsheets().values().get(spreadsheetId=sheet_id, range=read_range).execute()
    rows = result['values']
    column_names = rows[0]
    m = re.compile('[^\w\d]+')
    column_names = [re.sub(m, '_', i).strip().upper() for i in column_names]
    df = pd.DataFrame(rows[1:], columns=column_names)
    return df


def calculate(facility_code):
    ts = datetime.datetime.now()

    logger.info(f"Facility_code:{facility_code}")

    sheet_id = '1NcE4mXzJWqcm9V3GfiIH03-qPhxtpM23mmV4Ge2YI6g'

    othParams = get_gsheet_values(sheet_id, 'Other params', 'A1:C100')

    z = horus(othParams, facility_code)

    z.applicationData = z.invoke()

    output = z.prophesize()
    app_score = output['FinalPD']
    b_data = z.get_behavioural_data()
    result = {}
    result['time_stamp'] = ts
    #result['loan_id'] = z.loanID
    result['facility_code'] = z.facility_code
    result['borrower_id'] = z.borrowerID
    result['BS_PD'] = output['BS_PD']
    result['Pefindo_PD'] = output['Pefindo_PD']
    result['ApplicationPD'] = output['FinalPD']
    result['historicalDPD'] = z.historicalDPD
    result['percentage_dpd_total'] = list(b_data['percentage_dpd'])[0]
    result['customer_tenure_b'] = list(b_data['tenure'])[0]
    result['previous_application_pd'] = list(b_data['application_score_pd_one_year_before'])[0]
    result['percentage_full_payment_last_year'] = list(b_data['percentage_full_payments'])[0]
    result['number_of_applied_loans_last_year'] = list(b_data['number_of_applied_loans_last_year'])[0]  
    if list(b_data['application_score_pd_one_year_before'])[0]==None:
        result['change_in_pd'] =0
    else:
        result['change_in_pd'] = (result['ApplicationPD']-list(b_data['application_score_pd_one_year_before'])[0])*100/(list(b_data['application_score_pd_one_year_before'])[0]+0.0001)
    if b_data['tenure'][0]==None:
        b_data, pred_behavioural, a_b_indicator = b_data, None, "A"
        final_pd_ab = app_score
    elif b_data['tenure'][0]>=90:
        b_data, pred_behavioural = z.behavioural_prophesize()
        final_pd_ab = pred_behavioural
        a_b_indicator = 'B'
    else:
        b_data, pred_behavioural, a_b_indicator = b_data, None, "A"
        final_pd_ab = app_score

    output['application_data'] = z.applicationData.to_json()
    result['BehaviouralPD'] = pred_behavioural
    result['ABIndicator'] = a_b_indicator
    result['FinalPD'] = final_pd_ab
    logger.info(f"Application Data: {json.loads(output['application_data'])}")
    logger.info(f"Final Results: {result}")

    # updating CBM Table
    df_dwh = pd.DataFrame(columns=['LOAN_FACILITY_CODE', 'CMD_CTR_BORROWER_ID', 'LOAN_PRODUCT_NAME',
       'CMD_CTR_LOAN_ID', 'LOAN_CREATED_DATE', 'BANK_PD', 'PEFINDO_PD',
       'APPLICATION_PD', 'BEHAVIOURAL_PD', 'FINAL_PD', 'MODEL_TYPE',
       'MONTHLY_CREDIT', 'MONTHLY_DEBIT', 'MONTHLY_END_BALANCE', 'DC_RATIO',
       'BAL_CREDIT_RATIO', 'BAL_DEBIT_RATIO', 'MONTHLY_CREDIT_PCT',
       'MONTHLY_DEBIT_PCT', 'MONTHLY_END_BALANCE_PCT', 'DC_RATIO_PCT',
       'BAL_CREDIT_RATIO_PCT', 'BAL_DEBIT_RATIO_PCT', 'IDSCORE',
       'FASILITAS_AKTIF_JENIS_FASILITAS_BG_DITERBITKAN', 'MAKS_USIA_TUNGGAKAN', 'FASILITAS_AKTIF_JENIS_FASILITAS_LAINNYA', 'PERCENTAGE_DPD0',
       'PERCENTAGE_FULL_PAYMENTS_LAST_YEAR',
       'NUMBER_OF_APPLIED_LOANS_LAST_YEAR', 'CUSTOMER_TENURE',
       'PREVIOUS_APPLICATION_PD', 'CHANGE_IN_APPLICATION_PD','SCORING_TIMESTAMP'])
    if z.loan_date==None:
        z.loan_date = str(pd.to_datetime("today", utc=True))
    df_dwh.loc[0, 'LOAN_FACILITY_CODE'] = z.facility_code
    df_dwh.loc[0, 'CMD_CTR_BORROWER_ID'] = z.borrowerID
    df_dwh.loc[0, 'LOAN_PRODUCT_NAME'] = z.product_name
    df_dwh.loc[0, 'CMD_CTR_LOAN_ID'] = 0
    df_dwh.loc[0, 'LOAN_CREATED_DATE'] = str(z.loan_date)
    df_dwh.loc[0, 'BANK_PD'] = x['BS_PD']
    df_dwh.loc[0, 'PEFINDO_PD'] = x['Pefindo_PD']
    df_dwh.loc[0, 'APPLICATION_PD'] = x['ApplicationPD']
    df_dwh.loc[0, 'BEHAVIOURAL_PD'] = x['BehaviouralPD']
    df_dwh.loc[0, 'FINAL_PD'] = x['FinalPD']
    df_dwh.loc[0, 'MODEL_TYPE'] = x['ABIndicator']
    df_dwh[['IDSCORE',
           'FASILITAS_AKTIF_JENIS_FASILITAS_BG_DITERBITKAN', 'MAKS_USIA_TUNGGAKAN', 'FASILITAS_AKTIF_JENIS_FASILITAS_LAINNYA']] = z.pefindo_input.head(1)
    df_dwh[['MONTHLY_CREDIT', 'MONTHLY_DEBIT', 'MONTHLY_END_BALANCE', 'DC_RATIO',
           'BAL_CREDIT_RATIO', 'BAL_DEBIT_RATIO', 'MONTHLY_CREDIT_PCT',
           'MONTHLY_DEBIT_PCT', 'MONTHLY_END_BALANCE_PCT', 'DC_RATIO_PCT',
           'BAL_CREDIT_RATIO_PCT', 'BAL_DEBIT_RATIO_PCT']] = z.all_bs_data[['BANKCREDIT','BANKDEBIT','AVGBANKMONTHENDBALANCES','DCRATIO','BALANCECREDITRATIO',
           'BALANCEDEBITRATIO', 'BANKCREDITPCT', 'BANKDEBITPCT',
           'BANKENDBALANCESPCT', 'DCRATIOPCT', 'BALANCECREDITRATIOPCT','BALANCEDEBITRATIOPCT']]
    if x['percentage_dpd_total'] is None:
        df_dwh.loc[0, 'PERCENTAGE_DPD0'] = x['percentage_dpd_total']
    else:
        df_dwh.loc[0, 'PERCENTAGE_DPD0'] = x['percentage_dpd_total']/100
    df_dwh.loc[0, 'PERCENTAGE_FULL_PAYMENTS_LAST_YEAR'] = x['percentage_full_payment_last_year']
    df_dwh.loc[0, 'NUMBER_OF_APPLIED_LOANS_LAST_YEAR'] = x['number_of_applied_loans_last_year']
    df_dwh.loc[0, 'CUSTOMER_TENURE'] = x['customer_tenure_b']
    df_dwh.loc[0, 'PREVIOUS_APPLICATION_PD'] = x['previous_application_pd']
    df_dwh.loc[0, 'CHANGE_IN_APPLICATION_PD'] = x['change_in_pd']
    df_dwh.loc[0,'SCORING_TIMESTAMP'] = pd.to_datetime("today", utc=True)
    df_dwh[['MONTHLY_CREDIT', 'MONTHLY_DEBIT', 'MONTHLY_END_BALANCE', 'DC_RATIO',
       'BAL_CREDIT_RATIO','BAL_DEBIT_RATIO_PCT', 'IDSCORE',
       'FASILITAS_AKTIF_JENIS_FASILITAS_BG_DITERBITKAN', 'MAKS_USIA_TUNGGAKAN', 'FASILITAS_AKTIF_JENIS_FASILITAS_LAINNYA', 'PERCENTAGE_DPD0',
       'PERCENTAGE_FULL_PAYMENTS_LAST_YEAR',
       'NUMBER_OF_APPLIED_LOANS_LAST_YEAR', 'CUSTOMER_TENURE',
       'PREVIOUS_APPLICATION_PD', 'CHANGE_IN_APPLICATION_PD','BEHAVIOURAL_PD']] = df_dwh[['MONTHLY_CREDIT', 'MONTHLY_DEBIT', 'MONTHLY_END_BALANCE', 'DC_RATIO',
       'BAL_CREDIT_RATIO','BAL_DEBIT_RATIO_PCT', 'IDSCORE',
       'FASILITAS_AKTIF_JENIS_FASILITAS_BG_DITERBITKAN', 'MAKS_USIA_TUNGGAKAN', 'FASILITAS_AKTIF_JENIS_FASILITAS_LAINNYA', 'PERCENTAGE_DPD0',
       'PERCENTAGE_FULL_PAYMENTS_LAST_YEAR',
       'NUMBER_OF_APPLIED_LOANS_LAST_YEAR', 'CUSTOMER_TENURE',
       'PREVIOUS_APPLICATION_PD', 'CHANGE_IN_APPLICATION_PD','BEHAVIOURAL_PD']].replace([np.nan, '#N/A', 'NA', 'N.A', 'N/A', np.inf, np.NINF, None], 999999)
    df_dwh = df_dwh.replace([np.nan, '#N/A', 'NA', 'N.A', 'N/A', np.inf, np.NINF,"None"], "")
    df_dwh = df_dwh.head(1)
    write_to_snowflake(df_dwh)
    return result



if __name__ == "__main__":
    facility_code_zi = "APPZI0223-0016"
    facility_code_id = 'APPID0523-0285'
    calculate(facility_code_zi)
    # print("Error List: ", mod_err.error_list)
    # print("\n\n\n...........The End............")

# print(mod_err.error_list)
# json_load_test('thor.json')
# sys.exit()
