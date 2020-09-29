# VIP SAMII Code
# Written by Brenden McGale

# Call Libraries

library(tidyverse)
library(purrr)
library(odbc)
library(RODBC)
library(dplyr)
library(plotly)
library(ggplot2)
library(gtable)
library(stringi)
library(DBI)
library(RcppRoll)
library(naniar)
library(stringr)
library(lubridate)

# Establish ODBC Connection

con <- dbConnect(odbc::odbc(), dsn = "CLPImpala")

# Bring in All Tdlinx-Specific Unique Attributes for Mapping

VIP_SAM_Tdlinx <- dbSendQuery(con,"SELECT DISTINCT tdlinx_number, premise_type, bf_premise_type, bf_channel, channel, subchannel,
                                                   food_type, owner_name, ultimate_owner_code, ultimate_owner_name, account_parent,
                                                   account_indicator, account_active_flag, account_manager, acct_mgr_name, marketing_group_name,
                                                   marketing_group_code, chain, store_status, store_name, store_street_address, store_city,
                                                   store_state, store_zip, market_county, store_latitude, store_longitude, msa_code, msa_name,
                                                   dma_name, dma_code, sells_distilled_spirits, sells_wine, sells_beer, serves_alcohol_on_premise,
                                                   account_class_spirit, account_class_rtd, account_class_wine, noncorp_owned, bf_sells_spirits,
                                                   bf_sells_rtd, bf_sells_wine, sf_acct_owner_name, sf_acct_owner_mgnr_name, store_phone_number,
                                                   store_area_code, bfsee_activity_segment, bfsee_super_segment, bfsee_source,
                                                   bfsee_universe, market, market_name, market_lvl0, market_lvl1, market_lvl2, market_lvl3,
                                                   market_lvl4, market_lvl5, market_lvl6, market_lvl7, market_lvl8, market_lvl9
                                  FROM dp_nar.vwr_vip_sam
                                  WHERE value_type IN ('010')
                                  AND nar_excl_flag IS NULL
                                  AND brand_lvl1 IN ('Active Brands')
                                  AND market_lvl3 IN ('United States')
                                  AND brand_name NOT IN ('Bacardi','Bols','Maximus')
                                  AND CONCAT(source, market_name) NOT IN ('SAMMichigan')
                                  AND (transaction_type IS NULL OR transaction_type IN ('R'))")

# Execute Fetch to Read in the Data

VIP_SAM_Tdlinx <- dbFetch(VIP_SAM_Tdlinx)

# Set Blank Rows to NA to Allow Following Coalesce Function to Work

VIP_SAM_Tdlinx <- VIP_SAM_Tdlinx %>%
                  mutate_if(is.character, list(~na_if(., ""))) %>%
                  mutate_if(is.character, list(~na_if(., "  ")))

# Fill in All Hierarchy Values

VIP_SAM_Tdlinx <- VIP_SAM_Tdlinx %>%
                  mutate(market_lvl1 = coalesce(market_lvl1, market_lvl0),
                         market_lvl2 = coalesce(market_lvl2, market_lvl1),
                         market_lvl3 = coalesce(market_lvl3, market_lvl2),
                         market_lvl4 = coalesce(market_lvl4, market_lvl3),
                         market_lvl5 = coalesce(market_lvl5, market_lvl4),
                         market_lvl6 = coalesce(market_lvl6, market_lvl5),
                         market_lvl7 = coalesce(market_lvl7, market_lvl6),
                         market_lvl8 = coalesce(market_lvl8, market_lvl7),
                         market_lvl9 = coalesce(market_lvl9, market_lvl8))

# Replace all NA or Blank Character Fields with "N/A" & all NA Numeric Fields with 0

VIP_SAM_Tdlinx <- VIP_SAM_Tdlinx %>%
                  mutate_if(is.character, list(~na_if(., ""))) %>%
                  mutate_if(is.character, list(~na_if(., "  "))) %>%
                  
                  mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>%
                  mutate_if(is.character, ~ replace(.,is.na(.),"N/A")) %>%
                  mutate_if(is.character, str_replace_all, pattern = "^$", replacement = "N/A")

# Conduct One Final Step to Ensure All Tdlinx Attributes are Unique

VIP_SAM_Tdlinx <- VIP_SAM_Tdlinx %>%
                  group_by(tdlinx_number) %>%
                  
                  filter(tdlinx_number != "N/A") %>%
                  
                  mutate(Row_Number = row_number()) %>%
                  filter(Row_Number == 1) %>%
                  
                  ungroup() %>%
                  
                  arrange(tdlinx_number) %>%
                  select(-Row_Number)

# Establish ODBC Connection

con <- dbConnect(odbc::odbc(), dsn = "CLPImpala")

VIP_SAM_Brand <- dbSendQuery(con,"SELECT DISTINCT reporting_brand, brand_name, sub_brand, sub_brand_name, brand_lvl0,
                                                  brand_lvl1, brand_lvl2, brand_lvl3, brand_lvl4, brand_lvl5,
                                                  brand_lvl6, brand_us_lvl0, brand_us_lvl1, brand_us_lvl2, brand_us_lvl3,
                                                  brand_us_lvl4, brand_us_lvl5, brand_us_lvl6
                                  FROM dp_nar.vwr_vip_sam
                                  WHERE value_type IN ('010')
                                  AND nar_excl_flag IS NULL
                                  AND brand_lvl1 IN ('Active Brands')
                                  AND market_lvl3 IN ('United States')
                                  AND brand_name NOT IN ('Bacardi','Bols','Maximus')
                                  AND CONCAT(source, market_name) NOT IN ('SAMMichigan')
                                  AND (transaction_type IS NULL OR transaction_type IN ('R'))")

# Execute Fetch to Read in the Data

VIP_SAM_Brand <- dbFetch(VIP_SAM_Brand)

# Set Blank Rows to NA to Allow Following Coalesce Function to Work

VIP_SAM_Brand <- VIP_SAM_Brand %>%
                 mutate_if(is.character, list(~na_if(., ""))) %>%
                 mutate_if(is.character, list(~na_if(., " ")))

# Fill in All Hierarchy Values

VIP_SAM_Brand <- VIP_SAM_Brand %>%
                 mutate(brand_us_lvl1 = coalesce(brand_us_lvl1, brand_us_lvl0),
                        brand_us_lvl2 = coalesce(brand_us_lvl2, brand_us_lvl1),
                        brand_us_lvl3 = coalesce(brand_us_lvl3, brand_us_lvl2),
                        brand_us_lvl4 = coalesce(brand_us_lvl4, brand_us_lvl3),
                        brand_us_lvl5 = coalesce(brand_us_lvl5, brand_us_lvl4),
                        brand_us_lvl6 = coalesce(brand_us_lvl6, brand_us_lvl5),
                        brand_lvl1 = coalesce(brand_lvl1, brand_lvl0),
                        brand_lvl2 = coalesce(brand_lvl2, brand_lvl1),
                        brand_lvl3 = coalesce(brand_lvl3, brand_lvl2),
                        brand_lvl4 = coalesce(brand_lvl4, brand_lvl3),
                        brand_lvl5 = coalesce(brand_lvl5, brand_lvl4),
                        brand_lvl6 = coalesce(brand_lvl6, brand_lvl5))

# Replace all NA or Blank Character Fields with "N/A" & all NA Numeric Fields with 0

VIP_SAM_Brand <- VIP_SAM_Brand %>%
                 mutate_if(is.character, list(~na_if(., ""))) %>%
                 mutate_if(is.character, list(~na_if(., "  "))) %>%
                  
                 mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>%
                 mutate_if(is.character, ~ replace(.,is.na(.),"N/A")) %>%
                 mutate_if(is.character, str_replace_all, pattern = "^$", replacement = "N/A")

# Conduct One Final Step to Ensure All Brand Attributes are Unique

VIP_SAM_Reporting_Brand <- VIP_SAM_Brand %>%
                           select(-sub_brand, -sub_brand_name) %>%
                             
                           group_by(reporting_brand) %>%
                             
                           mutate(Row_Number = row_number()) %>%
                           filter(Row_Number == 1) %>%
                             
                           ungroup() %>%
                             
                           arrange(reporting_brand) %>%
                           select(-Row_Number)

VIP_SAM_Sub_Brand <- VIP_SAM_Brand %>%
                           select(sub_brand, sub_brand_name) %>%
                          
                           group_by(sub_brand) %>%
                          
                           mutate(Row_Number = row_number()) %>%
                           filter(Row_Number == 1) %>%
                          
                           ungroup() %>%
                          
                           arrange(sub_brand) %>%
                           select(-Row_Number)

rm(VIP_SAM_Brand)

# Establish ODBC Connection

con <- dbConnect(odbc::odbc(), dsn = "CLPImpala")

# Bring in All Bottle Size-Specific Unique Attributes for Mapping

VIP_SAM_Bottle_Size <- dbSendQuery(con,"SELECT DISTINCT bottle_size, bottle_size_us, bottle_size_name
                                        FROM dp_nar.vwr_vip_sam
                                        WHERE value_type IN ('010')
                                        AND nar_excl_flag IS NULL
                                        AND brand_lvl1 IN ('Active Brands')
                                        AND market_lvl3 IN ('United States')
                                        AND brand_name NOT IN ('Bacardi','Bols','Maximus')
                                        AND CONCAT(source, market_name) NOT IN ('SAMMichigan')
                                        AND (transaction_type IS NULL OR transaction_type IN ('R'))")

# Execute Fetch to Read in the Data

VIP_SAM_Bottle_Size <- dbFetch(VIP_SAM_Bottle_Size)

# Replace all NA or Blank Character Fields with "N/A" & all NA Numeric Fields with 0

VIP_SAM_Bottle_Size <- VIP_SAM_Bottle_Size %>%
                       mutate_if(is.character, list(~na_if(., ""))) %>%
                       mutate_if(is.character, list(~na_if(., "  "))) %>%
                        
                       mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>%
                       mutate_if(is.character, ~ replace(.,is.na(.),"N/A")) %>%
                       mutate_if(is.character, str_replace_all, pattern = "^$", replacement = "N/A")

# Conduct One Final Step to Ensure All Bottle Size Attributes are Unique

VIP_SAM_Bottle_Size <- VIP_SAM_Bottle_Size %>%
                       group_by(bottle_size) %>%
                        
                       mutate(Row_Number = row_number()) %>%
                       filter(Row_Number == 1) %>%
                        
                       ungroup() %>%
                        
                       arrange(bottle_size) %>%
                       select(-Row_Number)

# Establish ODBC Connection

con <- dbConnect(odbc::odbc(), dsn = "CLPImpala")

# Bring in All Ship To-Specific Unique Attributes for Mapping

VIP_SAM_Ship_To <- dbSendQuery(con,"SELECT DISTINCT ship_to, bf_customer_name, distributor_grouping,
                                                    dist_group_name, dist_fam_lvl0, dist_fam_lvl1,
                                                    dist_fam_lvl2, dist_fam_lvl3, dist_fam_lvl4
                                    FROM dp_nar.vwr_vip_sam
                                    WHERE value_type IN ('010')
                                    AND nar_excl_flag IS NULL
                                    AND brand_lvl1 IN ('Active Brands')
                                    AND market_lvl3 IN ('United States')
                                    AND brand_name NOT IN ('Bacardi','Bols','Maximus')
                                    AND CONCAT(source, market_name) NOT IN ('SAMMichigan')
                                    AND (transaction_type IS NULL OR transaction_type IN ('R'))")

# Execute Fetch to Read in the Data

VIP_SAM_Ship_To <- dbFetch(VIP_SAM_Ship_To)

# Set Blank Rows to NA to Allow Following Coalesce Function to Work

VIP_SAM_Ship_To <- VIP_SAM_Ship_To %>%
                   mutate_if(is.character, list(~na_if(., ""))) %>%
                   mutate_if(is.character, list(~na_if(., " ")))

# Fill in All Hierarchy Values

VIP_SAM_Ship_To <- VIP_SAM_Ship_To %>%
                   mutate(dist_fam_lvl1 = coalesce(dist_fam_lvl1, dist_fam_lvl0),
                          dist_fam_lvl2 = coalesce(dist_fam_lvl2, dist_fam_lvl1),
                          dist_fam_lvl3 = coalesce(dist_fam_lvl3, dist_fam_lvl2),
                          dist_fam_lvl4 = coalesce(dist_fam_lvl4, dist_fam_lvl3))

# Replace all NA or Blank Character Fields with "N/A" & all NA Numeric Fields with 0

VIP_SAM_Ship_To <- VIP_SAM_Ship_To %>%
                   mutate_if(is.character, list(~na_if(., ""))) %>%
                   mutate_if(is.character, list(~na_if(., "  "))) %>%
                    
                   mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>%
                   mutate_if(is.character, ~ replace(.,is.na(.),"N/A")) %>%
                   mutate_if(is.character, str_replace_all, pattern = "^$", replacement = "N/A")

# Conduct One Final Step to Ensure All Ship To Attributes are Unique

VIP_SAM_Ship_To <- VIP_SAM_Ship_To %>%
                   group_by(ship_to) %>%
                  
                   mutate(Row_Number = row_number()) %>%
                   filter(Row_Number == 1) %>%
                  
                   ungroup() %>%
                  
                   arrange(ship_to) %>%
                   select(-Row_Number)

# Establish ODBC Connection

con <- dbConnect(odbc::odbc(), dsn = "CLPImpala")

VIP_SAM <- dbSendQuery(con,"SELECT tdlinx_number, reporting_brand, sub_brand,
                                   bottle_size, ship_to, fy, fy_yr_mo, 
                                   SUM(cases_9l) AS cases_9l, 
                                   SUM(cases_9l_plan) AS cases_9l_plan, 
                                   SUM(cases_flat) AS cases_flat,
                                   SUM(value_usd) AS value_usd
                            FROM dp_nar.vwr_vip_sam
                            WHERE value_type IN ('010')
                            AND nar_excl_flag IS NULL
                            AND brand_lvl1 IN ('Active Brands')
                            AND market_lvl3 IN ('United States')
                            AND fy >= ((SELECT MAX(fy) FROM dp_nar.vwr_vip_sam)-3)
                            AND brand_name NOT IN ('Bacardi','Bols','Maximus')
                            AND CONCAT(source, market_name) NOT IN ('SAMMichigan')
                            AND (transaction_type IS NULL OR transaction_type IN ('R'))
                            GROUP BY tdlinx_number, reporting_brand, sub_brand,
                                     bottle_size, ship_to, fy, fy_yr_mo")

# Execute Fetch to Read in the Data

VIP_SAM <- dbFetch(VIP_SAM)

# Replace all NA or Blank Character Fields with "N/A" & all NA Numeric Fields with 0

VIP_SAM <- VIP_SAM %>%
  
           mutate_if(is.character, list(~na_if(., ""))) %>%
           mutate_if(is.character, list(~na_if(., " "))) %>%
                  
           mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>%
           mutate_if(is.character, ~ replace(.,is.na(.),"N/A")) %>%
           mutate_if(is.character, str_replace_all, pattern = "^$", replacement = "N/A")

# Build Date Field

VIP_SAM <- VIP_SAM %>%
           mutate(fy_yr_mo = as.character(fy_yr_mo)) %>%
           mutate(fy_month = as.numeric(str_sub(fy_yr_mo, -2, -1))) %>%

           select(-fy_yr_mo) %>%

           mutate(cy_month = case_when(fy_month == 1 ~ 5,
                                       fy_month == 2 ~ 6,
                                       fy_month == 3 ~ 7,
                                       fy_month == 4 ~ 8,
                                       fy_month == 5 ~ 9,
                                       fy_month == 6 ~ 10,
                                       fy_month == 7 ~ 11,
                                       fy_month == 8 ~ 12,
                                       fy_month == 9 ~ 1,
                                       fy_month == 10 ~ 2,
                                       fy_month == 11 ~ 3,
                                       fy_month == 12 ~ 4)) %>%
  
           mutate(cy = case_when(cy_month < 5 ~ fy,
                                 cy_month >= 5 ~ fy - 1)) %>%
  
           mutate(day = 1) %>%
           mutate(t_date = as.Date(make_date(cy, cy_month, day))) %>%

           select(-fy, -fy_month, -cy, -cy_month, -cy) %>%
  
           select(tdlinx_number, ship_to, reporting_brand, sub_brand,
                  bottle_size, t_date, cases_9l:value_usd) %>%
  
           arrange(tdlinx_number, ship_to, reporting_brand, sub_brand,
                   bottle_size, t_date)

VIP_SAM <- VIP_SAM %>%
           filter(tdlinx_number != "N/A")

VIP_SAM_Check <- VIP_SAM %>%
                 group_by(tdlinx_number, ship_to, reporting_brand,
                           sub_brand, bottle_size, t_date) %>%
                  
                 mutate(Row_Number = row_number()) %>%
                 mutate(Max_Row_Number = max(Row_Number)) %>%
                  
                 filter(Max_Row_Number >1) %>%
                  
                 ungroup() %>%
                  
                 arrange(tdlinx_number, ship_to, reporting_brand, 
                         sub_brand, bottle_size, t_date, Row_Number) %>%
                  
                 select(-Row_Number, -Max_Row_Number)

rm(VIP_SAM_Check)

# Create Unique Attribute List to Complete Data Frame with All Dates Per Unique Attribute

VIP_SAM_Attribute <- VIP_SAM %>%
                     select(tdlinx_number:bottle_size) %>%
                     distinct() %>%
  
                     arrange(tdlinx_number, ship_to, reporting_brand,
                             sub_brand, bottle_size)

# Create Comprehensive, Unique Date List to Bind with All Attributes (Complete Data Frame)

VIP_SAM_Date <- VIP_SAM %>%
                select(t_date) %>%
                distinct() %>%
  
                arrange(t_date) %>%
                
                mutate(t_date = paste("V",t_date, sep = "_"),
                       t_date_name = NA) %>%
                
                pivot_wider(names_from = t_date, values_from = t_date_name)

# Build Combined Data Frame (Shell) for Full Data Completion Across all Attributes

VIP_SAM_Complete <- bind_rows(VIP_SAM_Attribute, VIP_SAM_Date) %>%
  
                    pivot_longer(cols = starts_with("V_"), 
                                 names_to = "t_date",
                                 names_prefix = "V_") %>%
                    
                    select(-value) %>%
                    drop_na() %>%
                    
                    mutate(t_date = as.Date(t_date,"%Y-%m-%d" ))

# Remove Unnecessary Data Sets

rm(VIP_SAM_Attribute)
rm(VIP_SAM_Date)

# Merge Data Sets

VIP_SAM_Final <- full_join(VIP_SAM_Complete, VIP_SAM)

# Remove Unnecessary Data Sets

rm(VIP_SAM)
rm(VIP_SAM_Complete)

# Flag Added Data & Populate N/A Data with "N/A" for Character Fields and 0 for Numeric fields

VIP_SAM_Final <- VIP_SAM_Final %>%
                 mutate(Complete_Data_Flag = if_else(is.na(cases_9l), 1, 0)) %>%
                 mutate_if(is.character, replace_na, "N/A") %>%
                 mutate_if(is.numeric, replace_na, 0)

# Remove Unnecessary Fields

VIP_SAM_Final <- VIP_SAM_Final %>%
                 select(-cases_9l_plan)

# Build CY and FY Fields

VIP_SAM_Final <- VIP_SAM_Final %>%
                 mutate(cy = year(t_date),
                        cy_month = month(t_date),
                        fy = case_when(cy_month < 5 ~ cy,
                                      cy_month >= 5 ~ cy + 1)) %>%
 
                 select(-cy_month) %>%
                   
                 select(tdlinx_number, ship_to, reporting_brand, sub_brand,
                        bottle_size, t_date, cy, fy, Complete_Data_Flag, cases_9l, 
                        cases_flat, value_usd)

# Calculate Rolling Case Volume for This Year & Last Year & Reorder Dataset

VIP_SAM_Final <- VIP_SAM_Final %>% 
                 arrange(tdlinx_number, ship_to, reporting_brand, sub_brand,
                         bottle_size, t_date) %>% 
  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size, fy) %>% 
  
                 mutate(FYTD_cases_9l = cumsum(cases_9l)) %>% 
  
                 ungroup() %>%
  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size, cy) %>% 
  
                 mutate(CYTD_cases_9l = cumsum(cases_9l)) %>% 
                
                 ungroup() %>%
  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size) %>% 
  
                 mutate(V1_Month_cases_9l = cases_9l) %>% 
                 mutate(V3_Month_cases_9l = roll_sum(x = cases_9l, n = 3, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V6_Month_cases_9l = roll_sum(x = cases_9l, n = 6, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V12_Month_cases_9l = roll_sum(x = cases_9l, n = 12, align = "right", fill = 0, na.rm = FALSE)) %>%
                
                 mutate(FYTD_cases_9l_LY = replace_na(lag(FYTD_cases_9l, 12),0)) %>%
                 mutate(CYTD_cases_9l_LY = replace_na(lag(CYTD_cases_9l, 12),0)) %>%
                
                 mutate(V1_Month_cases_9l_LY = replace_na(lag(V1_Month_cases_9l, 12),0)) %>%
                 mutate(V3_Month_cases_9l_LY = replace_na(lag(V3_Month_cases_9l, 12),0)) %>%
                 mutate(V6_Month_cases_9l_LY = replace_na(lag(V6_Month_cases_9l, 12),0)) %>%
                 mutate(V12_Month_cases_9l_LY = replace_na(lag(V12_Month_cases_9l, 12),0)) %>%
  
                 ungroup() %>%
  
                 select(tdlinx_number, ship_to, reporting_brand, sub_brand, bottle_size,
                        t_date, cy, fy, Complete_Data_Flag, CYTD_cases_9l, CYTD_cases_9l_LY,
                        FYTD_cases_9l, FYTD_cases_9l_LY, V1_Month_cases_9l, V1_Month_cases_9l_LY,
                        V3_Month_cases_9l, V3_Month_cases_9l_LY, V6_Month_cases_9l, V6_Month_cases_9l_LY,
                        V12_Month_cases_9l, V12_Month_cases_9l_LY, -cases_9l, cases_flat, value_usd)

# Calculate Rolling Flat Case Volume for This Year & Last Year & Reorder Dataset

VIP_SAM_Final <- VIP_SAM_Final %>% 
                 arrange(tdlinx_number, ship_to, reporting_brand, sub_brand,
                         bottle_size, t_date) %>% 
                  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                           bottle_size, fy) %>% 
                  
                 mutate(FYTD_cases_flat = cumsum(cases_flat)) %>% 
                  
                 ungroup() %>%
                  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size, cy) %>% 
                  
                 mutate(CYTD_cases_flat = cumsum(cases_flat)) %>% 
                  
                 ungroup() %>%
                  
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size) %>% 
  
                 mutate(V1_Month_cases_flat = cases_flat) %>% 
                 mutate(V3_Month_cases_flat = roll_sum(x = cases_flat, n = 3, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V6_Month_cases_flat = roll_sum(x = cases_flat, n = 6, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V12_Month_cases_flat = roll_sum(x = cases_flat, n = 12, align = "right", fill = 0, na.rm = FALSE)) %>%
                  
                 mutate(FYTD_cases_flat_LY = replace_na(lag(FYTD_cases_flat, 12),0)) %>%
                 mutate(CYTD_cases_flat_LY = replace_na(lag(CYTD_cases_flat, 12),0)) %>%
                  
                 mutate(V1_Month_cases_flat_LY = replace_na(lag(V1_Month_cases_flat, 12),0)) %>%
                 mutate(V3_Month_cases_flat_LY = replace_na(lag(V3_Month_cases_flat, 12),0)) %>%
                 mutate(V6_Month_cases_flat_LY = replace_na(lag(V6_Month_cases_flat, 12),0)) %>%
                 mutate(V12_Month_cases_flat_LY = replace_na(lag(V12_Month_cases_flat, 12),0)) %>%
                  
                 ungroup() %>%
                  
                 select(tdlinx_number, ship_to, reporting_brand, sub_brand, bottle_size,
                        t_date, cy, fy, Complete_Data_Flag, CYTD_cases_9l:V12_Month_cases_9l_LY, CYTD_cases_flat, 
                        CYTD_cases_flat_LY, FYTD_cases_flat, FYTD_cases_flat_LY, V1_Month_cases_flat, 
                        V1_Month_cases_flat_LY, V3_Month_cases_flat, V3_Month_cases_flat_LY, V6_Month_cases_flat,
                        V6_Month_cases_flat_LY, V12_Month_cases_flat, V12_Month_cases_flat_LY, -cases_flat, value_usd)

# Calculate Rolling Value for This Year & Last Year & Reorder Dataset

VIP_SAM_Final <- VIP_SAM_Final %>% 
                 arrange(tdlinx_number, ship_to, reporting_brand, sub_brand,
                         bottle_size, t_date) %>% 
                
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size, fy) %>% 
                
                 mutate(FYTD_value_usd = cumsum(value_usd)) %>% 
                
                 ungroup() %>%
                
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size, cy) %>% 
                
                 mutate(CYTD_value_usd = cumsum(value_usd)) %>% 
                
                 ungroup() %>%
                
                 group_by(tdlinx_number, ship_to, reporting_brand, sub_brand,
                          bottle_size) %>% 
  
                 mutate(V1_Month_value_usd = value_usd) %>% 
                 mutate(V3_Month_value_usd = roll_sum(x = value_usd, n = 3, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V6_Month_value_usd = roll_sum(x = value_usd, n = 6, align = "right", fill = 0, na.rm = FALSE)) %>%
                 mutate(V12_Month_value_usd = roll_sum(x = value_usd, n = 12, align = "right", fill = 0, na.rm = FALSE)) %>%
                  
                 mutate(FYTD_value_usd_LY = replace_na(lag(FYTD_value_usd, 12),0)) %>%
                 mutate(CYTD_value_usd_LY = replace_na(lag(CYTD_value_usd, 12),0)) %>%
                  
                 mutate(V1_Month_value_usd_LY = replace_na(lag(V1_Month_value_usd, 12),0)) %>%
                 mutate(V3_Month_value_usd_LY = replace_na(lag(V3_Month_value_usd, 12),0)) %>%
                 mutate(V6_Month_value_usd_LY = replace_na(lag(V6_Month_value_usd, 12),0)) %>%
                 mutate(V12_Month_value_usd_LY = replace_na(lag(V12_Month_value_usd, 12),0)) %>%
                  
                 ungroup() %>%
  
                 select(tdlinx_number, ship_to, reporting_brand, sub_brand, bottle_size,
                        t_date, cy, fy, Complete_Data_Flag, CYTD_cases_9l:V12_Month_cases_9l_LY, CYTD_value_usd, 
                        CYTD_value_usd_LY, FYTD_value_usd, FYTD_value_usd_LY, V1_Month_value_usd, 
                        V1_Month_value_usd_LY, V3_Month_value_usd, V3_Month_value_usd_LY, V6_Month_value_usd,
                        V6_Month_value_usd_LY, V12_Month_value_usd, V12_Month_value_usd_LY, -value_usd) %>%
  
                 mutate_if(is.character, replace_na, "N/A") %>%
                 mutate_if(is.numeric, replace_na, 0)

# Develop Sales Indicators for This Year & Last Year

VIP_SAM_Final <- VIP_SAM_Final %>%
                 mutate(CYTD_Sales = case_when(CYTD_cases_9l > 0 ~ 1,
                                               CYTD_cases_9l <= 0 ~ 0)) %>%
  
                 mutate(CYTD_Sales_LY = case_when(CYTD_cases_9l_LY > 0 ~ 1,
                                                  CYTD_cases_9l_LY <= 0 ~ 0)) %>%
                   
                 mutate(FYTD_Sales = case_when(FYTD_cases_9l > 0 ~ 1,
                                               FYTD_cases_9l <= 0 ~ 0)) %>%
  
                 mutate(FYTD_Sales_LY = case_when(FYTD_cases_9l_LY > 0 ~ 1,
                                                  FYTD_cases_9l_LY <= 0 ~ 0)) %>%
      
                 mutate(V1_Month_Sales = case_when(V1_Month_cases_9l > 0 ~ 1,
                                                   V1_Month_cases_9l <= 0 ~ 0)) %>%
                    
                 mutate(V1_Month_Sales_LY = case_when(V1_Month_cases_9l_LY > 0 ~ 1,
                                                      V1_Month_cases_9l_LY <= 0 ~ 0)) %>%
                    
                 mutate(V3_Month_Sales = case_when(V3_Month_cases_9l > 0 ~ 1,
                                                   V3_Month_cases_9l <= 0 ~ 0)) %>%
                    
                 mutate(V3_Month_Sales_LY = case_when(V3_Month_cases_9l_LY > 0 ~ 1,
                                                      V3_Month_cases_9l_LY <= 0 ~ 0)) %>%
                    
                 mutate(V6_Month_Sales = case_when(V6_Month_cases_9l > 0 ~ 1,
                                                   V6_Month_cases_9l <= 0 ~ 0)) %>%
                    
                 mutate(V6_Month_Sales_LY = case_when(V6_Month_cases_9l_LY > 0 ~ 1,
                                                      V6_Month_cases_9l_LY <= 0 ~ 0)) %>%
                   
                 mutate(V12_Month_Sales = case_when(V12_Month_cases_9l > 0 ~ 1,
                                                    V12_Month_cases_9l <= 0 ~ 0)) %>%
                   
                 mutate(V12_Month_Sales_LY = case_when(V12_Month_cases_9l_LY > 0 ~ 1,
                                                       V12_Month_cases_9l_LY <= 0 ~ 0))

# Reorder Data

VIP_SAM_Final <- VIP_SAM_Final %>%
                 select(tdlinx_number:Complete_Data_Flag, CYTD_cases_9l:V12_Month_Sales_LY)


# Calculate Brand-Minor Level Repurchase Counts

VIP_SAM_Repurchase <- VIP_SAM_Final %>%
                      select(tdlinx_number, sub_brand, t_date, 
                             fy, cy, V1_Month_Sales) %>%
   
                      group_by(tdlinx_number, sub_brand, t_date) %>%
                      mutate(V1_Month_Purchase = max(V1_Month_Sales)) %>%
  
                      ungroup() %>%
  
                      select(-V1_Month_Sales) %>%
         
                      distinct() %>%

                      arrange(tdlinx_number, sub_brand, t_date) %>% 
                        
                      group_by(tdlinx_number, sub_brand, fy) %>% 
                  
                      mutate(FYTD_Repurchase = (cumsum(V1_Month_Purchase)-1)) %>% 
                        
                      ungroup() %>%
                        
                      group_by(tdlinx_number, sub_brand, cy) %>% 
                        
                      mutate(CYTD_Repurchase = (cumsum(V1_Month_Purchase) - 1)) %>% 
                        
                      ungroup() %>%
                        
                      group_by(tdlinx_number, sub_brand) %>%
                        
                      mutate(V1_Month_Repurchase = (V1_Month_Purchase - 1)) %>% 
                      mutate(V3_Month_Repurchase = (roll_sum(x = V1_Month_Purchase, n = 3, align = "right", fill = 0, na.rm = FALSE) -1)) %>%
                      mutate(V6_Month_Repurchase = (roll_sum(x = V1_Month_Purchase, n = 6, align = "right", fill = 0, na.rm = FALSE) -1)) %>%
                      mutate(V12_Month_Repurchase = (roll_sum(x = V1_Month_Purchase, n = 12, align = "right", fill = 0, na.rm = FALSE) -1)) %>%
                        
                      mutate(FYTD_Repurchase_LY = replace_na(lag(FYTD_Repurchase, 12),0)) %>%
                      mutate(CYTD_Repurchase_LY = replace_na(lag(CYTD_Repurchase, 12),0)) %>%
                        
                      mutate(V1_Month_Repurchase_LY = replace_na(lag(V1_Month_Repurchase, 12),0)) %>%
                      mutate(V3_Month_Repurchase_LY = replace_na(lag(V3_Month_Repurchase, 12),0)) %>%
                      mutate(V6_Month_Repurchase_LY = replace_na(lag(V6_Month_Repurchase, 12),0)) %>%
                      mutate(V12_Month_Repurchase_LY = replace_na(lag(V12_Month_Repurchase, 12),0)) %>%
                        
                      ungroup() %>%
    
                      select(-cy, -fy, -V1_Month_Purchase)
  
# Merge Repurchase Data Into Final Data Set

VIP_SAM_Final <- full_join(VIP_SAM_Final, VIP_SAM_Repurchase)

# Merge Tdlinx Data In

VIP_SAM_Final <- left_join(VIP_SAM_Final, VIP_SAM_Tdlinx, "tdlinx_number")

# Merge Ship To Data In

VIP_SAM_Final <- left_join(VIP_SAM_Final, VIP_SAM_Ship_To, "ship_to")

# Merge Bottle Size Data In

VIP_SAM_Final <- left_join(VIP_SAM_Final, VIP_SAM_Bottle_Size, "bottle_size")

# Merge Reporting Brand Data In

VIP_SAM_Final <- left_join(VIP_SAM_Final, VIP_SAM_Reporting_Brand, "reporting_brand")

# Merge Sub Brand Data In

VIP_SAM_Final <- left_join(VIP_SAM_Final, VIP_SAM_Sub_Brand, "sub_brand")

# Limit Final End Date to the Most Recent Closed Month

VIP_SAM_Final <- VIP_SAM_Final %>%
                 mutate(Timestamp = lubridate:: today()) %>%
                 mutate(Month = month(Timestamp)) %>%
                 mutate(Year = year(Timestamp)) %>%
                  
                 mutate(Date = paste(Month,"1",Year,sep = "-")) %>%
                 mutate(Date = as.Date(Date,"%m-%d-%Y")) %>%
                  
                 select(-Timestamp, -Month, -Year) %>%
                  
                 mutate(Month = month(Date) - 1) %>%
                 mutate(Year = year(Date)) %>%
                  
                 mutate(Date = paste(Month,"1",Year,sep = "-")) %>%
                 mutate(Date = as.Date(Date,"%m-%d-%Y")) %>%
                  
                 select(-Year, -Month) %>%
                  
                 filter(t_date <= Date) %>%
                 select(-Date)

VIP_SAM_Time_Flag <- VIP_SAM_Final %>%
                     select(t_date, fy, cy) %>%
  
                     mutate(Max_FY = max(fy)) %>%
                     mutate(Max_CY = max(cy)) %>%
  
                     distinct() %>%
                     arrange(t_date) %>%
  
                     mutate(Row_ID = row_number()) %>%
  
                     mutate(V1_Month_Actuals_Flag = if_else(Row_ID >= 1, 1, 0)) %>%
                     mutate(V3_Month_Actuals_Flag = if_else(Row_ID >= 3, 1, 0)) %>%
                     mutate(V6_Month_Actuals_Flag = if_else(Row_ID >= 6, 1, 0)) %>%
                     mutate(V12_Month_Actuals_Flag = if_else(Row_ID >= 12, 1, 0)) %>%
  
                     group_by(cy) %>%
  
                     mutate(Row_ID_CY = row_number()) %>%
                     mutate(Max_Row_ID_CY = max(Row_ID_CY)) %>%
  
                     ungroup() %>%

                     mutate(CYTD_Actuals_Flag = if_else(Max_Row_ID_CY == 12 | cy == Max_CY , 1, 0)) %>%

                     group_by(fy) %>%
                      
                     mutate(Row_ID_FY = row_number()) %>%
                     mutate(Max_Row_ID_FY = max(Row_ID_FY)) %>%
                      
                     ungroup() %>%
                      
                     mutate(FYTD_Actuals_Flag = if_else(Max_Row_ID_FY == 12 | fy == Max_FY , 1, 0)) %>%

                     mutate(V1_Month_YoY_Flag = if_else(Row_ID >= 13, 1, 0)) %>%
                     mutate(V3_Month_YoY_Flag = if_else(Row_ID >= 15, 1, 0)) %>%
                     mutate(V6_Month_YoY_Flag = if_else(Row_ID >= 18, 1, 0)) %>%
                     mutate(V12_Month_YoY_Flag = if_else(Row_ID >= 24, 1, 0)) %>%
  
                     mutate(Max_Row_ID_CY_LY = replace_na(lag(Max_Row_ID_CY, 12),0)) %>%
                     mutate(Max_Row_ID_FY_LY = replace_na(lag(Max_Row_ID_FY, 12),0)) %>%
  
                     mutate(CYTD_YoY_Flag = if_else(Max_Row_ID_CY_LY == 12, 1, 0)) %>%
                     mutate(FYTD_YoY_Flag = if_else(Max_Row_ID_FY_LY == 12, 1, 0)) %>%
  
                     select(-Row_ID, -Row_ID_CY, -Max_Row_ID_CY, -Row_ID_FY, 
                            -Max_Row_ID_FY, -Max_Row_ID_CY_LY, -Max_Row_ID_FY_LY,
                            -fy, -cy, -Max_FY, -Max_CY)

# Merge Time Period Flag Data Into Final Data Set

VIP_SAM_Final <- full_join(VIP_SAM_Final, VIP_SAM_Time_Flag)

# Develop Store Name & Tdlinx Field & SKU Field

VIP_SAM_Final <- VIP_SAM_Final %>%

mutate(SKU = paste(sub_brand_name, bottle_size_name, sep = " ")) %>%
mutate(Store = paste(store_name, tdlinx_number, sep = " - "))

# Reorder Final Data Set

VIP_SAM_Final <- VIP_SAM_Final %>%
                 select(tdlinx_number:fy, premise_type:sub_brand_name,
                        Store, SKU, Complete_Data_Flag:FYTD_YoY_Flag)

# Remove All Unnecessary Data

rm(VIP_SAM_Tdlinx)
rm(VIP_SAM_Ship_To)
rm(VIP_SAM_Bottle_Size)

rm(VIP_SAM_Reporting_Brand)
rm(VIP_SAM_Sub_Brand)
rm(VIP_SAM_Repurchase)
rm(VIP_SAM_Time_Flag)

# Set Working Directory for File Saving
  
setwd("C:/Users/bi6ejdr/Desktop/Robbins Work")

# Write Out CSV and Drop Trigger

write_csv(VIP_SAM_Final,"VIP_SAM_Test.csv")

                