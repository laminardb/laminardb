# FpML support matrix

Generated from `crates/laminar-connectors/fpml/coverage.toml` and the golden outputs by the
coverage test; do not edit by hand (`FPML_GOLDEN_REGEN=1` with the `fpml` feature rewrites
it). Closure `fpml-5-13-8-rec-2/confirmation`. Typed: 50 of 50 products, 54 of 54 loan representations, 90 of 90 document roots.

Rows are golden outputs of the bundled profile for that representation: official FpML
examples, and LaminarDB synthetic fixtures where the official corpus has none. Synthetic
fixtures are test evidence, not venue certification.

## Products

| Class | Element | Schema type | Profile | Column | Official rows | Synthetic rows | Synthetic fixtures |
|---|---|---|---|---|---|---|---|
| interest-rate | `bondOption` | `BondOption` | `fpml-5-13-confirmation-rates` | `bond_option` | 5 | 0 |  |
| equity | `brokerEquityOption` | `BrokerEquityOption` | `fpml-5-13-confirmation-equity-options` | `broker_equity_option` | 3 | 0 |  |
| interest-rate | `bulletPayment` | `BulletPayment` | `fpml-5-13-confirmation-rates` | `bullet_payment` | 1 | 0 |  |
| interest-rate | `capFloor` | `CapFloor` | `fpml-5-13-confirmation-rates` | `cap_floor` | 3 | 0 |  |
| commodity | `commodityBasketOption` | `CommodityBasketOption` | `fpml-5-13-confirmation-commodity` | `commodity_basket_option` | 1 | 0 |  |
| commodity | `commodityDigitalOption` | `CommodityDigitalOption` | `fpml-5-13-confirmation-commodity` | `commodity_digital_option` | 1 | 0 |  |
| commodity | `commodityForward` | `CommodityForward` | `fpml-5-13-confirmation-commodity` | `commodity_forward` | 4 | 0 |  |
| commodity | `commodityOption` | `CommodityOption` | `fpml-5-13-confirmation-commodity` | `commodity_option` | 11 | 0 |  |
| commodity | `commodityPerformanceSwap` | `CommodityPerformanceSwap` | `fpml-5-13-confirmation-commodity` | `commodity_performance_swap` | 4 | 0 |  |
| commodity | `commoditySwap` | `CommoditySwap` | `fpml-5-13-confirmation-commodity` | `commodity_swap` | 19 | 0 |  |
| commodity | `commoditySwaption` | `CommoditySwaption` | `fpml-5-13-confirmation-commodity` | `commodity_swaption` | 5 | 0 |  |
| equity | `correlationSwap` | `CorrelationSwap` | `fpml-5-13-confirmation-equity-volatility` | `correlation_swap` | 4 | 0 |  |
| credit | `creditDefaultSwap` | `CreditDefaultSwap` | `fpml-5-13-confirmation-credit` | `credit_default_swap` | 55 | 0 |  |
| credit | `creditDefaultSwapOption` | `CreditDefaultSwapOption` | `fpml-5-13-confirmation-credit` | `credit_default_swap_option` | 5 | 0 |  |
| equity | `dividendSwapOptionTransactionSupplement` | `DividendSwapOptionTransactionSupplement` | `fpml-5-13-confirmation-equity-swaps` | `dividend_swap_option_transaction_supplement` | 1 | 0 |  |
| equity | `dividendSwapTransactionSupplement` | `DividendSwapTransactionSupplement` | `fpml-5-13-confirmation-equity-swaps` | `dividend_swap_transaction_supplement` | 3 | 0 |  |
| equity | `equityAccumulator` | `EquityAccumulator` | `fpml-5-13-confirmation-equity-options` | `equity_accumulator` | 0 | 1 | equity-accumulator.xml |
| equity | `equityForward` | `EquityForward` | `fpml-5-13-confirmation-equity-options` | `equity_forward` | 1 | 0 |  |
| equity | `equityOption` | `EquityOption` | `fpml-5-13-confirmation-equity-options` | `equity_option` | 17 | 0 |  |
| equity | `equityOptionTransactionSupplement` | `EquityOptionTransactionSupplement` | `fpml-5-13-confirmation-equity-options` | `equity_option_transaction_supplement` | 12 | 0 |  |
| equity | `equitySwapTransactionSupplement` | `EquitySwapTransactionSupplement` | `fpml-5-13-confirmation-equity-swaps` | `equity_swap_transaction_supplement` | 10 | 0 |  |
| interest-rate | `fra` | `Fra` | `fpml-5-13-confirmation-rates` | `fra` | 3 | 0 |  |
| foreign-exchange | `fxAccrualDigitalOption` | `FxAccrualDigitalOption` | `fpml-5-13-confirmation-fx` | `fx_accrual_digital_option` | 1 | 0 |  |
| foreign-exchange | `fxAccrualForward` | `FxAccrualForward` | `fpml-5-13-confirmation-fx` | `fx_accrual_forward` | 10 | 0 |  |
| foreign-exchange | `fxAccrualOption` | `FxAccrualOption` | `fpml-5-13-confirmation-fx` | `fx_accrual_option` | 3 | 0 |  |
| foreign-exchange | `fxDigitalOption` | `FxDigitalOption` | `fpml-5-13-confirmation-fx` | `fx_digital_option` | 6 | 0 |  |
| foreign-exchange | `fxFlexibleForward` | `FxFlexibleForward` | `fpml-5-13-confirmation-fx` | `fx_flexible_forward` | 1 | 0 |  |
| foreign-exchange | `fxForwardVolatilityAgreement` | `FxForwardVolatilityAgreement` | `fpml-5-13-confirmation-fx` | `fx_forward_volatility_agreement` | 1 | 0 |  |
| foreign-exchange | `fxOption` | `FxOption` | `fpml-5-13-confirmation-fx` | `fx_option` | 8 | 0 |  |
| foreign-exchange | `fxRangeAccrual` | `FxRangeAccrual` | `fpml-5-13-confirmation-fx` | `fx_range_accrual` | 2 | 0 |  |
| foreign-exchange | `fxSingleLeg` | `FxSingleLeg` | `fpml-5-13-confirmation-fx` | `fx_single_leg` | 14 | 0 |  |
| foreign-exchange | `fxSwap` | `FxSwap` | `fpml-5-13-confirmation-fx` | `fx_swap` | 3 | 0 |  |
| foreign-exchange | `fxTargetKnockoutForward` | `FxTargetKnockoutForward` | `fpml-5-13-confirmation-fx` | `fx_target_knockout_forward` | 15 | 0 |  |
| foreign-exchange | `fxVarianceSwap` | `FxPerformanceSwap` | `fpml-5-13-confirmation-fx` | `fx_variance_swap` | 1 | 0 |  |
| foreign-exchange | `fxVolatilitySwap` | `FxPerformanceSwap` | `fpml-5-13-confirmation-fx` | `fx_volatility_swap` | 1 | 0 |  |
| cross-asset | `genericProduct` | `GenericProduct` | `fpml-5-13-confirmation-cross-asset` | `generic_product` | 0 | 1 | generic-product.xml |
| securities | `instrumentTradeDetails` | `InstrumentTradeDetails` | `fpml-5-13-confirmation-securities` | `instrument_trade_details` | 3 | 0 |  |
| securities-financing | `repo` | `Repo` | `fpml-5-13-confirmation-securities` | `repo` | 9 | 0 |  |
| equity | `returnSwap` | `ReturnSwap` | `fpml-5-13-confirmation-equity-swaps` | `return_swap` | 14 | 0 |  |
| securities-financing | `securityLending` | `SecurityLending` | `fpml-5-13-confirmation-securities` | `security_lending` | 2 | 0 |  |
| cross-asset | `standardProduct` | `StandardProduct` | `fpml-5-13-confirmation-cross-asset` | `standard_product` | 0 | 1 | standard-product.xml |
| cross-asset | `strategy` | `Strategy` | `fpml-5-13-confirmation-cross-asset` | `strategy` | 4 | 0 |  |
| interest-rate | `swap` | `Swap` | `fpml-5-13-confirmation-rates` | `swap` | 77 | 0 |  |
| interest-rate | `swaption` | `Swaption` | `fpml-5-13-confirmation-rates` | `swaption` | 10 | 0 |  |
| loans-and-deposits | `termDeposit` | `TermDeposit` | `fpml-5-13-confirmation-fx` | `term_deposit` | 3 | 0 |  |
| equity | `varianceOptionTransactionSupplement` | `VarianceOptionTransactionSupplement` | `fpml-5-13-confirmation-equity-volatility` | `variance_option_transaction_supplement` | 2 | 0 |  |
| equity | `varianceSwap` | `VarianceSwap` | `fpml-5-13-confirmation-equity-volatility` | `variance_swap` | 4 | 0 |  |
| equity | `varianceSwapTransactionSupplement` | `VarianceSwapTransactionSupplement` | `fpml-5-13-confirmation-equity-volatility` | `variance_swap_transaction_supplement` | 1 | 0 |  |
| equity | `volatilitySwap` | `VolatilitySwap` | `fpml-5-13-confirmation-equity-volatility` | `volatility_swap` | 0 | 1 | volatility-swap.xml |
| equity | `volatilitySwapTransactionSupplement` | `VolatilitySwapTransactionSupplement` | `fpml-5-13-confirmation-equity-volatility` | `volatility_swap_transaction_supplement` | 2 | 0 |  |

## Loan facilities and events

| Class | Element | Schema type | Profile | Column | Official rows | Synthetic rows | Synthetic fixtures |
|---|---|---|---|---|---|---|---|
| facilityGroup | `delayedDraw` | `DelayedDraw` | `fpml-5-13-confirmation-loan-facilities` | `facility` | 2 | 0 |  |
| facilityGroup | `letterOfCreditFacility` | `LetterOfCreditFacility` | `fpml-5-13-confirmation-loan-facilities` | `facility` | 0 | 1 | loan-letterOfCreditFacility.xml |
| facilityGroup | `revolver` | `Revolver` | `fpml-5-13-confirmation-loan-facilities` | `facility` | 0 | 1 | loan-revolver.xml |
| facilityGroup | `termLoan` | `TermLoan` | `fpml-5-13-confirmation-loan-facilities` | `facility` | 2 | 0 |  |
| facilityEventGroup | `accrualOptionChange` | `AccrualOptionChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 2 | 0 |  |
| facilityEventGroup | `accruingFeeChange` | `AccruingFeeChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 3 | 0 |  |
| facilityEventGroup | `accruingFeeExpiry` | `AccruingFeeExpiry` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `accruingFeePayment` | `AccruingFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 3 | 0 |  |
| facilityEventGroup | `accruingPikPayment` | `AccruingPikPayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `amendmentFeePayment` | `AmendmentFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `commitmentAdjustment` | `CommitmentAdjustment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `defaultRateChange` | `DefaultRateChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `defaultRateExpiry` | `DefaultRateExpiry` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `facilityExtensionFeePayment` | `FacilityExtensionFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `facilityPrepayment` | `FacilityPrepayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `facilityPrepaymentFeePayment` | `FacilityPrepaymentFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 0 | 1 | loan-facilityPrepaymentFeePayment.xml |
| facilityEventGroup | `facilityTermination` | `FacilityTermination` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `fundingFeePayment` | `FundingFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `mandatoryCostRateChange` | `MandatoryCostRateChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `mandatoryCostRateExpiry` | `MandatoryCostRateExpiry` | `fpml-5-13-confirmation-loan-servicing` | `event` | 0 | 1 | loan-mandatoryCostRateExpiry.xml |
| facilityEventGroup | `miscFeePayment` | `MiscFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `penaltyRateChange` | `PenaltyRateChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `penaltyRateExpiry` | `PenaltyRateExpiry` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `upfrontFeePayment` | `UpfrontFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| facilityEventGroup | `waiverFeePayment` | `WaiverFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanContractEventGroup | `baseRateSet` | `LoanContractBaseRateSet` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanContractEventGroup | `borrowing` | `Borrowing` | `fpml-5-13-confirmation-loan-servicing` | `event` | 2 | 0 |  |
| loanContractEventGroup | `breakageFeePayment` | `BreakageFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 0 | 1 | loan-breakageFeePayment.xml |
| loanContractEventGroup | `interestCapitalization` | `InterestCapitalization` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanContractEventGroup | `interestPayment` | `LoanInterestPayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 18 | 0 |  |
| loanContractEventGroup | `legacyInterestCapitalization` | `LegacyInterestCapitalization` | `fpml-5-13-confirmation-loan-servicing` | `event` | 0 | 1 | loan-legacyInterestCapitalization.xml |
| loanContractEventGroup | `legacyInterestPayment` | `LegacyLoanInterestPayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 0 | 1 | loan-legacyInterestPayment.xml |
| loanContractEventGroup | `loanContractAdjustment` | `LoanContractAdjustment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanContractEventGroup | `maturityChange` | `LoanContractMaturityChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanContractEventGroup | `repayment` | `Repayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 2 | 0 |  |
| loanContractEventGroup | `rollover` | `Rollover` | `fpml-5-13-confirmation-loan-servicing` | `event` | 13 | 0 |  |
| lcEventGroup | `lcAdjustment` | `LcAdjustment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| lcEventGroup | `lcFxRevaluation` | `LcFxRevaluation` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| lcEventGroup | `lcIssuance` | `LcIssuance` | `fpml-5-13-confirmation-loan-servicing` | `event` | 4 | 0 |  |
| lcEventGroup | `lcIssuanceFeePayment` | `LcIssuanceFeePayment` | `fpml-5-13-confirmation-loan-servicing` | `event` | 3 | 0 |  |
| lcEventGroup | `lcRateChange` | `LcRateChange` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| lcEventGroup | `lcRenewal` | `LcRenewal` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| lcEventGroup | `lcTermination` | `LcTermination` | `fpml-5-13-confirmation-loan-servicing` | `event` | 1 | 0 |  |
| loanAllocationEventGroup | `loanAllocation` | `LoanAllocationEvent` | `fpml-5-13-confirmation-loan-trading` | `event` | 3 | 0 |  |
| loanAllocationEventGroup | `loanAllocationConfirmation` | `LoanAllocationEvent` | `fpml-5-13-confirmation-loan-trading` | `event` | 3 | 0 |  |
| loanAllocationEventGroup | `loanAllocationFeeDue` | `LoanAllocationTransferFeeDue` | `fpml-5-13-confirmation-loan-trading` | `event` | 2 | 0 |  |
| loanAllocationEventGroup | `loanAllocationFeeOwed` | `LoanAllocationTransferFeeOwed` | `fpml-5-13-confirmation-loan-trading` | `event` | 2 | 0 |  |
| loanAllocationEventGroup | `loanAllocationSettlement` | `LoanAllocationSettlement` | `fpml-5-13-confirmation-loan-trading` | `event` | 9 | 0 |  |
| loanAllocationEventGroup | `loanAllocationSettlementDateAvailability` | `LoanAllocationSettlementDateAvailability` | `fpml-5-13-confirmation-loan-trading` | `event` | 5 | 0 |  |
| loanAllocationEventGroup | `loanAllocationSettlementDateFinalization` | `LoanAllocationSettlementDateFinalization` | `fpml-5-13-confirmation-loan-trading` | `event` | 5 | 0 |  |
| loanTradeEventGroup | `loanTrade` | `LoanTradeEvent` | `fpml-5-13-confirmation-loan-trading` | `event` | 6 | 0 |  |
| loanTradeEventGroup | `loanTradeConfirmation` | `LoanTradeEvent` | `fpml-5-13-confirmation-loan-trading` | `event` | 2 | 0 |  |
| loanTradeEventGroup | `loanTradeFeeDue` | `LoanTradeTransferFeeDue` | `fpml-5-13-confirmation-loan-trading` | `event` | 3 | 0 |  |
| loanTradeEventGroup | `loanTradeFeeOwed` | `LoanTradeTransferFeeOwed` | `fpml-5-13-confirmation-loan-trading` | `event` | 3 | 0 |  |

## Documents

| Kind | Element | Schema type | Profile | Column | Official rows | Synthetic rows | Synthetic fixtures |
|---|---|---|---|---|---|---|---|
| document | `allocationAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-allocationAcknowledgement.xml |
| document | `allocationApproved` | `AllocationApproved` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-allocationApproved.xml |
| document | `allocationException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-allocationException.xml |
| document | `allocationRefused` | `AllocationRefused` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-allocationRefused.xml |
| document | `approvalStatusNotification` | `ApprovalStatusNotification` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `clearingAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-clearingAcknowledgement.xml |
| document | `clearingConfirmed` | `ClearingConfirmed` | `fpml-5-13-confirmation-document` | `document_root` | 9 | 0 |  |
| document | `clearingEligibility` | `ClearingEligibility` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `clearingEligibilityAcknowledgement` | `Acknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-clearingEligibilityAcknowledgement.xml |
| document | `clearingEligibilityException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-clearingEligibilityException.xml |
| document | `clearingException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-clearingException.xml |
| document | `clearingRefused` | `ClearingRefused` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-clearingRefused.xml |
| document | `clearingStatus` | `ClearingStatus` | `fpml-5-13-confirmation-document` | `document_root` | 3 | 0 |  |
| document | `collateralAllocationAccepted` | `CollateralAllocationAccepted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-collateralAllocationAccepted.xml |
| document | `collateralAllocationAcknowledgement` | `Acknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-collateralAllocationAcknowledgement.xml |
| document | `collateralAllocationRejected` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-collateralAllocationRejected.xml |
| document | `confirmationAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-confirmationAcknowledgement.xml |
| document | `confirmationAgreed` | `ConfirmationAgreed` | `fpml-5-13-confirmation-document` | `document_root` | 9 | 0 |  |
| document | `confirmationDisputed` | `ConfirmationDisputed` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-confirmationDisputed.xml |
| document | `confirmationException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-confirmationException.xml |
| document | `confirmationStatus` | `ConfirmationStatus` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `consentAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-consentAcknowledgement.xml |
| document | `consentException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-consentException.xml |
| document | `consentGranted` | `ConsentGranted` | `fpml-5-13-confirmation-document` | `document_root` | 4 | 0 |  |
| document | `consentRefused` | `ConsentRefused` | `fpml-5-13-confirmation-document` | `document_root` | 2 | 0 |  |
| document | `creditEventAcknowledgement` | `Acknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-creditEventAcknowledgement.xml |
| document | `creditEventException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-creditEventException.xml |
| document | `creditEventNotification` | `CreditEventNotification` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-creditEventNotification.xml |
| document | `creditEventNotificationRetracted` | `CreditEventNotificationRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-creditEventNotificationRetracted.xml |
| document | `dataDocument` | `DataDocument` | `fpml-5-13-confirmation-document` | `document_root` | 175 | 0 |  |
| document | `dealStatement` | `DealStatement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `eventStatusException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-eventStatusException.xml |
| document | `eventStatusResponse` | `EventStatusResponse` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-eventStatusResponse.xml |
| document | `executionAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-executionAcknowledgement.xml |
| document | `executionAdvice` | `ExecutionAdvice` | `fpml-5-13-confirmation-document` | `document_root` | 21 | 0 |  |
| document | `executionAdviceAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-executionAdviceAcknowledgement.xml |
| document | `executionAdviceException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-executionAdviceException.xml |
| document | `executionAdviceRetracted` | `ExecutionAdviceRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `executionException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `executionNotification` | `ExecutionNotification` | `fpml-5-13-confirmation-document` | `document_root` | 67 | 0 |  |
| document | `executionRetracted` | `ExecutionRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `facilityNotification` | `FacilityNotification` | `fpml-5-13-confirmation-document` | `document_root` | 23 | 2 |  |
| document | `facilityPositionStatement` | `FacilityPositionStatement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `facilityStatement` | `FacilityStatement` | `fpml-5-13-confirmation-document` | `document_root` | 2 | 2 |  |
| document | `lcNotification` | `LcNotification` | `fpml-5-13-confirmation-document` | `document_root` | 12 | 0 |  |
| document | `loanAllocationNotification` | `LoanAllocationNotification` | `fpml-5-13-confirmation-document` | `document_root` | 23 | 0 |  |
| document | `loanBulkServicingNotification` | `LoanBulkServicingNotification` | `fpml-5-13-confirmation-document` | `document_root` | 14 | 0 |  |
| document | `loanContractNotification` | `LoanContractNotification` | `fpml-5-13-confirmation-document` | `document_root` | 4 | 3 |  |
| document | `loanCovenantObligationNotification` | `LoanCovenantObligationNotification` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanLegalActionNotification` | `LoanLegalActionNotification` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanLegalActionStatement` | `LoanLegalActionStatement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanNotificationAcknowledgement` | `LoanNotificationAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanNotificationException` | `LoanNotificationException` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanNotificationRetracted` | `LoanNotificationRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanPartyProfileStatement` | `LoanPartyProfileStatement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `loanTradeNotification` | `LoanTradeNotification` | `fpml-5-13-confirmation-document` | `document_root` | 18 | 0 |  |
| document | `maturityAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-maturityAcknowledgement.xml |
| document | `maturityException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-maturityException.xml |
| document | `maturityNotification` | `MaturityNotification` | `fpml-5-13-confirmation-document` | `document_root` | 2 | 0 |  |
| document | `messageRejected` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-messageRejected.xml |
| document | `optionExpirationNotification` | `ExecutionNotification` | `fpml-5-13-confirmation-document` | `document_root` | 4 | 0 |  |
| document | `outstandingContractsStatement` | `OutstandingContractsStatement` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `requestAllocation` | `RequestAllocation` | `fpml-5-13-confirmation-document` | `document_root` | 2 | 0 |  |
| document | `requestAllocationRetracted` | `RequestAllocationRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `requestClearing` | `RequestClearing` | `fpml-5-13-confirmation-document` | `document_root` | 4 | 0 |  |
| document | `requestClearingEligibility` | `RequestClearingEligibility` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `requestClearingRetracted` | `RequestClearingRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestClearingRetracted.xml |
| document | `requestCollateralAllocation` | `RequestCollateralAllocation` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestCollateralAllocation.xml |
| document | `requestConfirmation` | `RequestConfirmation` | `fpml-5-13-confirmation-document` | `document_root` | 102 | 4 |  |
| document | `requestConfirmationRetracted` | `ConfirmationRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestConfirmationRetracted.xml |
| document | `requestConsent` | `RequestConsent` | `fpml-5-13-confirmation-document` | `document_root` | 9 | 0 |  |
| document | `requestConsentRetracted` | `RequestConsentRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestConsentRetracted.xml |
| document | `requestEventStatus` | `RequestEventStatus` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestEventStatus.xml |
| document | `requestExecution` | `RequestExecution` | `fpml-5-13-confirmation-document` | `document_root` | 5 | 0 |  |
| document | `requestExecutionRetracted` | `RequestExecutionRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestExecutionRetracted.xml |
| document | `requestRetransmission` | `RequestRetransmission` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestRetransmission.xml |
| document | `requestTradeReferenceInformationUpdate` | `RequestTradeReferenceInformationUpdate` | `fpml-5-13-confirmation-document` | `document_root` | 1 | 0 |  |
| document | `requestTradeReferenceInformationUpdateRetracted` | `RequestTradeReferenceInformationUpdateRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-requestTradeReferenceInformationUpdateRetracted.xml |
| document | `serviceNotification` | `ServiceNotification` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-serviceNotification.xml |
| document | `serviceNotificationException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-serviceNotificationException.xml |
| document | `tradeChangeAdvice` | `TradeChangeAdvice` | `fpml-5-13-confirmation-document` | `document_root` | 2 | 0 |  |
| document | `tradeChangeAdviceAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-tradeChangeAdviceAcknowledgement.xml |
| document | `tradeChangeAdviceException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-tradeChangeAdviceException.xml |
| document | `tradeChangeAdviceRetracted` | `TradeChangeAdviceRetracted` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-tradeChangeAdviceRetracted.xml |
| document | `tradeReferenceInformationUpdateAcknowledgement` | `EventRequestAcknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-tradeReferenceInformationUpdateAcknowledgement.xml |
| document | `tradeReferenceInformationUpdateException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-tradeReferenceInformationUpdateException.xml |
| document | `valuationDocument` | `ValuationDocument` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-valuationDocument.xml |
| document | `verificationStatusAcknowledgement` | `Acknowledgement` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-verificationStatusAcknowledgement.xml |
| document | `verificationStatusException` | `Exception` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-verificationStatusException.xml |
| document | `verificationStatusNotification` | `VerificationStatusNotification` | `fpml-5-13-confirmation-document` | `document_root` | 0 | 1 | document-verificationStatusNotification.xml |
