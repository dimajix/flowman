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
            array = path("QualityReportedTransactions.TransactionData"),
            outerColumns = Explode.Columns(
                keep = path("metadata"),
                drop = path("metadata.correlationIds")
            )
        ),
        "transaction_updates" := Assemble(
            input = output("transaction_array"),
            columns = Assemble.Flatten(
                drop = Seq(
                    path("ErrorInfo"),
                    path("AirPlusMerchant.Address"),
                    path("Booker.Address"),
                    path("CarRental"),
                    path("CommissionPassback"),
                    path("Contact"),
                    path("CreditCardMerchant.Address"),
                    path("Description"),
                    path("Fee"),
                    path("FinancialInformation"),
                    path("Card.AidaCardUser.Address"),
                    path("Flight"),
                    path("GroundTransportation"),
                    path("HotelStay"),
                    path("MICE"),
                    path("OtherItem"),
                    path("Purchase"),
                    path("RailJourney"),
                    path("ShipJourney"),
                    path("Supplier.Address"),
                    path("Tax"),
                    path("Toll"),
                    path("TravelAgencyBooking")
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
