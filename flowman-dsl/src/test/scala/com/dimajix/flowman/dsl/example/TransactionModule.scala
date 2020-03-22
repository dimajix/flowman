package com.dimajix.flowman.dsl.example

import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.mapping.Assemble
import com.dimajix.flowman.dsl.mapping.Conform
import com.dimajix.flowman.dsl.mapping.Explode
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.types.TimestampType


object TransactionModule extends Module with ModuleCommon {
    mappings := (
        "transaction_array" := Explode(
            input = output("qualityreport_events"),
            array = col("QualityReportedTransactions.TransactionData"),
            outerColumns = Explode.Columns(
                keep = col("metadata"),
                drop = col("metadata.correlationIds")
            )
        ),
        "transaction_updates" := Assemble(
            input = output("transaction_array"),
            columns = Assemble.Flatten(
                drop = Seq(
                    col("ErrorInfo"),
                    col("AirPlusMerchant.Address"),
                    col("Booker.Address"),
                    col("CarRental"),
                    col("CommissionPassback"),
                    col("Contact"),
                    col("CreditCardMerchant.Address"),
                    col("Description"),
                    col("Fee"),
                    col("FinancialInformation"),
                    col("Card.AidaCardUser.Address"),
                    col("Flight"),
                    col("GroundTransportation"),
                    col("HotelStay"),
                    col("MICE"),
                    col("OtherItem"),
                    col("Purchase"),
                    col("RailJourney"),
                    col("ShipJourney"),
                    col("Supplier.Address"),
                    col("Tax"),
                    col("Toll"),
                    col("TravelAgencyBooking")
                )
            )
        ),
        "transaction" := Conform(
            input = output("transaction_updates"),
            naming = CaseFormat.SNAKE_CASE,
            types = Map("date" -> TimestampType)
        )
    )
    modules += (
        LatestAndHistory("transaction")
    )
}
