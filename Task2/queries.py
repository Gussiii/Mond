def get_query_item_per_sku():
    return """select Combine_SKU,
                    POS,
                    POSLag1,
                    POSLag2,
                    POSLag3,
                    POSLag4,
                    Month,
                    Year,
                    Day,
                    WeekHols,
                    WeekSeason,
                    NYE,
                    AUS_DAY,
                    V_Day,
                    Easter,
                    M_DAY,
                    CBRY_CUP,
                    FOOTY_FNL,
                    HLWEEN,
                    XMAS,
                    Price20Percent_OFF,
                    Price30Percent_OFF,
                    Price4Percent_OFF,
                    Price40Percent_OFF,
                    Price50Percent_OFF,
                    Price20Percent_OFFLag1,
                    Price30Percent_OFFLag1,
                    Price4Percent_OFFLag1,
                    Price40Percent_OFFLag1,
                    Price50Percent_OFFLag1,
                    Bench,
                    CAT,
                    INS,
                    BenchLag1,
                    CATLag1,
                    INSLag1,
                    END_FLY_BUYS,
                    END_1,
                    END_2,
                    END_3,
                    END_4,
                    END_6,
                    END_7,
                    END_8,
                    END_9,
                    END_10,
                    END_11,
                    END_12,
                    END_13,
                    END_14,
                    END_15,
                    END_16,
                    END_17,
                    END_18,
                    END_19,
                    END_20,
                    END_21,
                    END_22,
                    END_23,
                    END_24,
                    END_25,
                    END_26,
                    END_27,
                    END_28,
                    END_29,
                    END_30,
                    END_31,
                    END_32,
                    END_33,
                    END_34,
                    END_35,
                    END_36,
                    END_37,
                    END_38,
                    END_39,
                    END_40,
                    END_41,
                    END_42,
                    END_43,
                    END_44,
                    END_45,
                    count(*) from Sales1 group by Combine_SKU,
                    POS,
                    POSLag1,
                    POSLag2,
                    POSLag3,
                    POSLag4,
                    Month,
                    Year,
                    Day,
                    WeekHols,
                    WeekSeason,
                    NYE,
                    AUS_DAY,
                    V_Day,
                    Easter,
                    M_DAY,
                    CBRY_CUP,
                    FOOTY_FNL,
                    HLWEEN,
                    XMAS,
                    Price20Percent_OFF,
                    Price30Percent_OFF,
                    Price4Percent_OFF,
                    Price40Percent_OFF,
                    Price50Percent_OFF,
                    Price20Percent_OFFLag1,
                    Price30Percent_OFFLag1,
                    Price4Percent_OFFLag1,
                    Price40Percent_OFFLag1,
                    Price50Percent_OFFLag1,
                    Bench,
                    CAT,
                    INS,
                    BenchLag1,
                    CATLag1,
                    INSLag1,
                    END_FLY_BUYS,
                    END_1,
                    END_2,
                    END_3,
                    END_4,
                    END_6,
                    END_7,
                    END_8,
                    END_9,
                    END_10,
                    END_11,
                    END_12,
                    END_13,
                    END_14,
                    END_15,
                    END_16,
                    END_17,
                    END_18,
                    END_19,
                    END_20,
                    END_21,
                    END_22,
                    END_23,
                    END_24,
                    END_25,
                    END_26,
                    END_27,
                    END_28,
                    END_29,
                    END_30,
                    END_31,
                    END_32,
                    END_33,
                    END_34,
                    END_35,
                    END_36,
                    END_37,
                    END_38,
                    END_39,
                    END_40,
                    END_41,
                    END_42,
                    END_43,
                    END_44,
                    END_45 order by Combine_SKU
                """

def get_query_derin_ds():
    return """SELECT Combine_SKU,
                POS,
                POSLag1,
                POSLag2,
                POSLag3,
                POSLag4,
                Month,
                Year,
                Day,
                WeekHols,
                WeekSeason,
                NYE,
                AUS_DAY,
                V_Day,
                Easter,
                M_DAY,
                CBRY_CUP,
                FOOTY_FNL,
                HLWEEN,
                XMAS,
                Price20Percent_OFF,
                Price30Percent_OFF,
                Price4Percent_OFF,
                Price40Percent_OFF,
                Price50Percent_OFF,
                Price20Percent_OFFLag1,
                Price30Percent_OFFLag1,
                Price4Percent_OFFLag1,
                Price40Percent_OFFLag1,
                Price50Percent_OFFLag1,
                Bench,
                CAT,
                INS,
                BenchLag1,
                CATLag1,
                INSLag1,
                END_FLY_BUYS,
                END_1,
                END_2,
                END_3,
                END_4,
                END_6,
                END_7,
                END_8,
                END_9,
                END_10,
                END_11,
                END_12,
                END_13,
                END_14,
                END_15,
                END_16,
                END_17,
                END_18,
                END_19,
                END_20,
                END_21,
                END_22,
                END_23,
                END_24,
                END_25,
                END_26,
                END_27,
                END_28,
                END_29,
                END_30,
                END_31,
                END_32,
                END_33,
                END_34,
                END_35,
                END_36,
                END_37,
                END_38,
                END_39,
                END_40,
                END_41,
                END_42,
                END_43,
                END_44,
                END_45,
                date as ds,
                sum(Sales) as y FROM Sales1 GROUP BY Combine_SKU,
                POS,
                POSLag1,
                POSLag2,
                POSLag3,
                POSLag4,
                Month,
                Year,
                Day,
                WeekHols,
                WeekSeason,
                NYE,
                AUS_DAY,
                V_Day,
                Easter,
                M_DAY,
                CBRY_CUP,
                FOOTY_FNL,
                HLWEEN,
                XMAS,
                Price20Percent_OFF,
                Price30Percent_OFF,
                Price4Percent_OFF,
                Price40Percent_OFF,
                Price50Percent_OFF,
                Price20Percent_OFFLag1,
                Price30Percent_OFFLag1,
                Price4Percent_OFFLag1,
                Price40Percent_OFFLag1,
                Price50Percent_OFFLag1,
                Bench,
                CAT,
                INS,
                BenchLag1,
                CATLag1,
                INSLag1,
                END_FLY_BUYS,
                END_1,
                END_2,
                END_3,
                END_4,
                END_6,
                END_7,
                END_8,
                END_9,
                END_10,
                END_11,
                END_12,
                END_13,
                END_14,
                END_15,
                END_16,
                END_17,
                END_18,
                END_19,
                END_20,
                END_21,
                END_22,
                END_23,
                END_24,
                END_25,
                END_26,
                END_27,
                END_28,
                END_29,
                END_30,
                END_31,
                END_32,
                END_33,
                END_34,
                END_35,
                END_36,
                END_37,
                END_38,
                END_39,
                END_40,
                END_41,
                END_42,
                END_43,
                END_44,
                END_45,
                ds ORDER BY Combine_SKU,
                ds
                """
