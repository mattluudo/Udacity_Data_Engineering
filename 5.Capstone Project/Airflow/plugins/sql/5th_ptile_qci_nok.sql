WITH
  --Table to lookup market and submarket of each cell
  --Note: Add and remove markets/submarket as needed to speed up query
  market AS (
    SELECT DISTINCT
      MARKETCLUSTER AS MARKET,
      MARKET AS SUBMARKET,
      EUTRANCELL_NAME

    FROM PDW_MOBILITYSCORECARD_VIEWS.LTE_RTB_SCORECARD_DY
    WHERE 
      EUCELLVENDORLIST = 'N' AND MARKET <> '*' AND MARKET <> '*COMMERCIAL' 
	  AND MARKET <> '-' AND MARKET NOT LIKE '%-PART' AND EUTRANCELL_NAME <> '*' 
	  AND DATETIMELOCAL >= '2020-02-01' 
  )
  



SELECT
  --EXTRACT(WEEK FROM (tput.DATETIME-1)) as 'WEEK',
  --TRUNC(tput.DATETIME, 'HH') AS DATETIME,
  --TRUNC(tput.DATETIME) AS DATETIME,
  tput.DATETIME AS DATETIME,
  market.MARKET AS MARKET,
  market.SUBMARKET AS SUBMARKET,
  'N' AS VENDOR,
  market.EUTRANCELL_NAME AS EUTRANCELL,
  
  --RRC Connections
  ue.SUM_RRC_CONNECTED_UE AS AVG_RRC_CONN_NUM,
  ue.DENOM_RRC_CONNECTED_UE AS AVG_RRC_CONN_DEN,
  ue.SUM_RRC_CONNECTED_UE/NULLIFZERO(ue.DENOM_RRC_CONNECTED_UE) AS AVG_RRC_CONN,
  
  --TOTAL TP
  (ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_6) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_7) 
    + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_8) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_9))/1000 AS DL_DRB_TP_NUM,
  (ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_6) + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_7)
    + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_8) + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_9))/1000 AS DL_DRB_TP_DEN,
  (ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_6) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_7) 
    + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_8) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_9))
    / (NULLIFZERO(ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_6) + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_7)
    + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_8) + ZEROIFNULL(tput.IP_TPUT_time_DL_QCI_9))) AS DL_DRB_TP,
  
  --QCI TP
  tput.IP_TPUT_VOL_DL_QCI_6/1000 AS QCI6_DL_TPUT_NUM,
  tput.IP_TPUT_VOL_DL_QCI_7/1000 AS QCI7_DL_TPUT_NUM,
  tput.IP_TPUT_VOL_DL_QCI_8/1000 AS QCI8_DL_TPUT_NUM,
  tput.IP_TPUT_VOL_DL_QCI_9/1000 AS QCI9_DL_TPUT_NUM,
  
  tput.IP_TPUT_time_DL_QCI_6/1000 AS QCI6_DL_TPUT_DEN,
  tput.IP_TPUT_time_DL_QCI_7/1000 AS QCI7_DL_TPUT_DEN,
  tput.IP_TPUT_time_DL_QCI_8/1000 AS QCI8_DL_TPUT_DEN,
  tput.IP_TPUT_time_DL_QCI_9/1000 AS QCI9_DL_TPUT_DEN,
  
  tput.IP_TPUT_VOL_DL_QCI_6/NULLIFZERO(tput.IP_TPUT_time_DL_QCI_6) AS DL_DRB_QCI6_TP,
  tput.IP_TPUT_VOL_DL_QCI_7/NULLIFZERO(tput.IP_TPUT_time_DL_QCI_7) AS DL_DRB_QCI7_TP,
  tput.IP_TPUT_VOL_DL_QCI_8/NULLIFZERO(tput.IP_TPUT_time_DL_QCI_8) AS DL_DRB_QCI8_TP,
  tput.IP_TPUT_VOL_DL_QCI_9/NULLIFZERO(tput.IP_TPUT_time_DL_QCI_9) AS DL_DRB_QCI9_TP,
  
  --ERAB CONNECTIONS
  ZEROIFNULL(load.ERAB_INI_SETUP_SUCC_QCI6) + ZEROIFNULL(load.ERAB_ADD_SETUP_SUCC_QCI6) AS QCI6_ERAB_SETUP_SUCC,
  ZEROIFNULL(load.ERAB_INI_SETUP_SUCC_QCI7) + ZEROIFNULL(load.ERAB_ADD_SETUP_SUCC_QCI7) AS QCI7_ERAB_SETUP_SUCC,  
  ZEROIFNULL(load.ERAB_INI_SETUP_SUCC_QCI8) + ZEROIFNULL(load.ERAB_ADD_SETUP_SUCC_QCI8) AS QCI8_ERAB_SETUP_SUCC,
  ZEROIFNULL(load.ERAB_INI_SETUP_SUCC_QCI9) + ZEROIFNULL(load.ERAB_ADD_SETUP_SUCC_QCI9) AS QCI9_ERAB_SETUP_SUCC
  
  
FROM 
  PDW_NOKEUTRAN_VIEWS.V_LTE_CELL_THROUGHPUT_DY tput,
  PDW_NOKEUTRAN_VIEWS.V_LTE_EPS_BEARER_DY load,
  PDW_NOKEUTRAN_VIEWS.V_LTE_UE_QUANTITY_DY ue,
  market
  
WHERE 
  tput.EUTRANCELL = market.EUTRANCELL_NAME AND
  
  tput.DATETIME = load.DATETIME AND
  tput.ENODEB = load.ENODEB AND
  tput.EUTRANCELL = load.EUTRANCELL AND

  tput.DATETIME = ue.DATETIME AND
  tput.ENODEB = ue.ENODEB AND
  tput.EUTRANCELL = ue.EUTRANCELL AND
 
  tput.DATETIME >= '{start_date}'  AND tput.DATETIME < '{end_date}' AND
  load.DATETIME >= '{start_date}'  AND load.DATETIME < '{end_date}' AND
  ue.DATETIME >= '{start_date}'  AND ue.DATETIME < '{end_date}' AND
  
  (ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_6) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_7) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_8) + ZEROIFNULL(tput.IP_TPUT_VOL_DL_QCI_9) > 0)
  


ORDER BY
  MARKET, 
  SUBMARKET, 
  DATETIME, 
  EUTRANCELL_NAME


