-- ============================================================================
-- PHASE 8: SQL Views & Indexes
-- File: sql/views/05_v_DM_CustomerRFM_ByTenant.sql
-- Description: View RFM phan tich khach hang cho tenant.
--              Tra ve phan khuc RFM, diem churn, doanh thu 90/30 ngay.
-- ============================================================================

SET NOCOUNT ON;
GO

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_CustomerRFM_ByTenant')
BEGIN
    DROP VIEW v_DM_CustomerRFM_ByTenant;
END
GO

CREATE VIEW v_DM_CustomerRFM_ByTenant
AS
SELECT
    r.RFMKey,
    r.TenantID,
    r.CustomerKey,
    r.CustomerCode,
    r.FullName,
    r.City,
    r.CustomerType,
    r.LoyaltyTier,
    r.RecencyScore,
    r.FrequencyScore,
    r.MonetaryScore,
    r.RecencyDays,
    r.FrequencyOrders,
    r.MonetaryAmount,
    r.AvgOrderValue,
    r.RFMScore,
    r.RFMScoreGrade,
    r.Segment,
    r.SegmentDesc,
    r.LastPurchaseDate,
    r.FirstPurchaseDate,
    r.CustomerLifetimeDays,
    r.Last90DaysRevenue,
    r.Last30DaysRevenue,
    r.ChurnRiskScore,
    r.LastRefreshed,

    -- Dimension: Customer info
    c.CustomerType AS CustomerTypeDetail,
    c.LoyaltyTier AS LoyaltyTierDetail,
    c.MemberSince,
    c.LoyaltyPoint

FROM DM_CustomerRFM r
INNER JOIN DimCustomer c ON c.CustomerKey = r.CustomerKey
    AND c.TenantID = r.TenantID AND c.IsCurrent = 1
WHERE r.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20););
GO

PRINT 'Created view: v_DM_CustomerRFM_ByTenant';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_DM_CustomerRFM_SegmentSummary
-- Description: View tong hop so luong khach hang theo tung Segment.
--              Dung cho Pie Chart / Donut Chart tren Superset dashboard.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_CustomerRFM_SegmentSummary')
BEGIN
    DROP VIEW v_DM_CustomerRFM_SegmentSummary;
END
GO

CREATE VIEW v_DM_CustomerRFM_SegmentSummary
AS
SELECT
    TenantID,
    Segment,
    COUNT(*) AS CustomerCount,
    SUM(MonetaryAmount) AS TotalMonetary,
    AVG(RFMScore) AS AvgRFMScore,
    AVG(CAST(ChurnRiskScore AS DECIMAL(5,2)) AS AvgChurnRiskScore,
    SUM(Last90DaysRevenue) AS Total90DayRevenue,
    SUM(Last30DaysRevenue) AS Total30DayRevenue,
    MAX(ChurnRiskScore) AS MaxChurnRisk
FROM DM_CustomerRFM
WHERE TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
GROUP BY TenantID, Segment;
GO

PRINT 'Created view: v_DM_CustomerRFM_SegmentSummary';
GO


-- ============================================================================
-- PHASE 8: SQL Views — v_DM_CustomerRFM_AtRisk
-- Description: View chi hien thi khach hang nguy co (ChurnRiskScore > 30).
--              Dung cho Dashboard Churn Prevention.
-- ============================================================================

IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_DM_CustomerRFM_AtRisk')
BEGIN
    DROP VIEW v_DM_CustomerRFM_AtRisk;
END
GO

CREATE VIEW v_DM_CustomerRFM_AtRisk
AS
SELECT
    r.RFMKey,
    r.TenantID,
    r.CustomerKey,
    r.CustomerCode,
    r.FullName,
    r.City,
    r.LoyaltyTier,
    r.RecencyDays,
    r.FrequencyOrders,
    r.MonetaryAmount,
    r.LastPurchaseDate,
    r.RFMScore,
    r.RFMScoreGrade,
    r.Segment,
    r.ChurnRiskScore,
    r.LastRefreshed,

    c.Phone,
    c.Email

FROM DM_CustomerRFM r
INNER JOIN DimCustomer c ON c.CustomerKey = r.CustomerKey
    AND c.TenantID = r.TenantID AND c.IsCurrent = 1
WHERE r.TenantID = CAST(SESSION_CONTEXT('tenant_id') AS VARCHAR(20);)
  AND r.ChurnRiskScore > 30
ORDER BY r.ChurnRiskScore DESC, r.RecencyDays DESC;
GO

PRINT 'Created view: v_DM_CustomerRFM_AtRisk';
GO