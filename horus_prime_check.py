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
        self.quantumCap = None
        self.tenorCap = None
        self.modelInput = None
        self.min_quantum = 3000  # Minimum quantum for bolt product
        # Note: Not hardcoded, fetching them from gsheet. It is just initialization.
        self.ead = .60
        self.lgd = 1
        self.int_floor = None
        self.int_limit = None
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

    def get_borrower_id(self):

        if str(self.platform_code).upper() == "ZI" or str(self.platform_code).upper() == "ZU":
            DBLoan = productionDBLoan
        else:
            DBLoan = productionDBLoan_id
        try:
            if str(self.product_name).lower() != "tl enterprise":
                borrower_id = DBLoan.query_postgres('''
                            SELECT borrower_id
                            FROM loans
                            WHERE facility_code=%(facility_code)s;
                        ''', params={'facility_code': self.facility_code})['borrower_id'].values[0]
            else:
                borrower_id = DBLoan.query_postgres('''
                                            SELECT borrower_id
                                            FROM loans
                                            WHERE loan_code=%(facility_code)s;
                                        ''', params={'facility_code': self.facility_code})['borrower_id'].values[0]


            print("Borrower ID", borrower_id)
        except:
            print("Borrower ID from the Gsheet")
            borrower_id = self.borrowerID_gs

        return borrower_id

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
    output['application_data'] = z.applicationData.to_json()

    logger.info(f"Application Data: {json.loads(output['application_data'])}")
    result = {}
    result['time_stamp'] = ts
    #result['loan_id'] = z.loanID
    result['facility_code'] = z.facility_code
    result['borrower_id'] = z.borrowerID
    result['BS_PD'] = output['BS_PD']
    result['Pefindo_PD'] = output['Pefindo_PD']
    result['FinalPD'] = output['FinalPD']
    result['historicalDPD'] = z.historicalDPD
    logger.info(f"Final Results: {result}")
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
