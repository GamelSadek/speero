const startCronJob = require('speero-backend/helpers/start.cron.job')
const Helpers = require('speero-backend/helpers')
const Invoice = require('speero-backend/modules/invoices')
const DirectOrder = require('speero-backend/modules/direct.orders')
const Part = require('speero-backend/modules/parts')
const DirectOrderPart = require('speero-backend/modules/direct.order.parts')

async function createInvoice() {
    try {
        const directOrderParts = await getDirectOrderParts()
        const allPs = await getAllParts()
        const allParts = concatenateArrays(allPs, directOrderParts)
        const directOrderPartsGroups = getDirectOrderPartsGroups(allParts, 'directOrderId')
        const directOrderPartsIds = getObjectIds(directOrderPartsGroups)
        const directOrdersWithInvoices = await getDirectOrdersWithInvoicesFromDb(directOrderPartsIds)
        const invoicesIds = await calculateAndInsertInvoices(directOrdersWithInvoices, directOrderPartsGroups)
        return { case: 1, message: 'invoices created successfully.', invoicesIds: invoicesIds }
    } catch (err) {
        Helpers.reportError(err)
    }
}
async function getDirectOrderParts() {
    return DirectOrderPart.Model.find({
        createdAt: { $gt: new Date('2021-04-01') },
        fulfillmentCompletedAt: { $exists: true },
        invoiceId: { $exists: false }
    }
     ,).select('_id directOrderId partClass priceBeforeDiscount')
}
async function getAllParts() {
    return Part.Model.find(
        {
            directOrderId: { $exists: true },
            createdAt: { $gt: new Date('2021-04-01') },
            partClass: 'requestPart',
            pricedAt: { $exists: true },
            invoiceId: { $exists: false }
        }).select('_id directOrderId partClass premiumPriceBeforeDiscount')
}
function concatenateArrays(arr1, arr2) {
    return arr1.concat(arr2)
}
function getDirectOrderPartsGroups(arr, key) {
    let directOrderPartsGroups = {}
    arr.forEach(element => {
        if (directOrderPartsGroups[element[key]]) {
            directOrderPartsGroups[element[key]].push(element)
        }
        else {
            directOrderPartsGroups[element[key]] = []
            directOrderPartsGroups[element[key]].push(element)
        }
    })
    return directOrderPartsGroups
}
function getObjectIds(obj) {
    return Object.keys(obj)
}
async function getDirectOrdersWithInvoicesFromDb(ids) {
    return DirectOrder.Model.aggregate([
        {
            '$match': { '_id': { $in: ids } }
        },
        {
            '$lookup': {
                'from': 'invoces',
                'localField': '_id',
                'foreignField': 'directOrderId',
                'as': 'invoices'
            }
        },
        {
            '$project': {
                '_id': 1,
                'partsIds': 1,
                'requestPartsIds': 1,
                'discountAmount': 1,
                'deliveryFees': 1,
                'walletPaymentAmount': 1,
                'invoices': {
                    'walletPaymentAmount': 1,
                    'discountAmount': 1,
                    'deliveryFees': 1
                }
            }
        }
    ])
}
function handleDirectOrderHasDeliveryFeesAndNoInvoices(deliveryFees, totalAmount) {
    if (deliveryFees && dirctOrder.invoces.length === 0) {
        totalAmount += deliveryFees
    }
    return totalAmount
}
function handlePaymentAmount(directOrder, fieldName, totalAmount, fieldValue) {
    if (directOrder[fieldName]) {
        directOrder.invoces.forEach(invoice => {
            fieldValue = Math.min(0, fieldValue - invoice[fieldName])
        })
        fieldValue = Math.min(fieldValue, totalAmount)
        totalAmount -= directOrder[field]
    }
    return { totalAmount, fieldValue }
}
function handleTotalAmountLessThanZero(totalAmount) {
    if (totalAmount < 0) {
        throw Error(`Could not create invoice for directOrder: ${directOrder._id} with totalAmount: ${totalAmount}. `)
    }
}
async function bulkUpdateModels(invoices) {
    let bulkUpdateDirectOrder = []
    let bulkUpdateDirectOrderPart = []
    let bulkUpdateParts = []
    invoices.forEach(invoice => {
        bulkUpdateDirectOrder.push({
            updateOne: {
                filter: {
                    _id: invoice.directOrder
                },
                update: {
                    $addToSet: {
                        invoicesIds: invoice._id
                    }
                }
            }
        })
        bulkUpdateDirectOrderPart.push({
            updatMany: {
                filter: {
                    _id: { $in: [invoice.directOrderPartsIds] }
                },
                update: {
                    invoiceId: invoice._id
                }
            }
        })
        bulkUpdateParts.push({
            updatMany: {
                filter: {
                    _id: { $in: [invoice.requestPartsIds] }
                },
                update: {
                    invoiceId: invoice._id
                }
            }
        })
    })
    await DirectOrder.Model.bulkWrite(bulkUpdateDirectOrder)
    await DirectOrderPart.Model.bulkWrite(bulkUpdateDirectOrderPart)
    await Part.Model.bulkWrite(bulkUpdateParts)
}
function calculatePricesAndGetIds(directOrderPartsGroups, directOrderId) {
    let dpsPrice, rpsPrice = 0
    let dpsId = []
    let rpsId = []
    directOrderPartsGroups[directOrderId].forEach(part => {
        if (part.partClass === 'StockPart' ||
            part.partClass === 'QuotaPart') {
            dpsPrice += part.priceBeforeDiscount
            dpsId.push(directOrderId)
        }
        else {
            rpsPrice += part.premiumPriceBeforeDiscount
            rpsId.push(directOrderId)
        }
    })
    return {
        dpsPrice,
        rpsPrice,
        dpsId,
        rpsId
    }
}
function handleSpecialCalculations(totalAmount, deliveryFees, walletPaymentAmount, dirctOrder, discountAmount) {
    totalAmount = handleDirectOrderHasDeliveryFeesAndNoInvoices(deliveryFees, totalAmount)
    const totalAmountAndWalletPaymentAmount = handlePaymentAmount(dirctOrder, 'walletPaymentAmount', totalAmount, walletPaymentAmount)
    totalAmount = totalAmountAndWalletPaymentAmount.totalAmount
    walletPaymentAmount = totalAmountAndWalletPaymentAmount.totalAmount
    const totalAmountAndDiscountAmount = handlePaymentAmount(dirctOrder, 'discountAmount', totalAmount, discountAmount)
    totalAmount = totalAmountAndDiscountAmount.totalAmount
    discountAmount = totalAmountAndDiscountAmount.fieldValue
    return { totalAmount, walletPaymentAmount, discountAmount }
}
async function calculateAndInsertInvoices(directOrdersWithInvoices, directOrderPartsGroups) {
    let invoices = []
    directOrdersWithInvoices.forEach(directOrder => {
        const pricesAndIds = calculatePricesAndGetIds(directOrderPartsGroups, directOrder)
        const { dpsPrice, rpsPrice, dpsId, rpsId } = pricesAndIds
        const totalPrice = Helpers.Numbers.toFixedNumber(rpsPrice + dpsPrice)
        let totalAmount = totalPrice
        const { deliveryFees } = directOrder;
        let { walletPaymentAmount, discountAmount } = directOrder;
        const calculations = handleSpecialCalculations(totalAmount,
            deliveryFees,
            walletPaymentAmount,
            dirctOrder,
            discountAmount)
        totalAmount = calculations.totalAmount
        walletPaymentAmount = calculations.walletPaymentAmount
        discountAmount = calculations.walletPaymentAmount
        handleTotalAmountLessThanZero(totalAmount)
        invoices.push({
            directOrderId: directOrder._id,
            directOrderPartsIds: dpsId,
            requestPartsIds: rpsId,
            totalPartsAmount: totalPrice,
            totalAmount,
            deliveryFees,
            walletPaymentAmount,
            discountAmount
        })
    })
    const invoices = await Invoice.Model.insertMany(invoices)
    await bulkUpdateModels(invoices)
    return invoices.insertedIds
}

startCronJob('*/1 * * * *', createInvoice, true) // at 00:00 every day

module.exports = createInvoice
