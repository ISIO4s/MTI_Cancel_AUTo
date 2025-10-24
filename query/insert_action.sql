INSERT INTO xininsure.SALEACTION (
    saleid,
    SEQUENCE,
    duedate,
    actionid,
    actionstatus,
    REQUESTREMARK,
    ACTIONREMARK
) VALUES (
    :SALEID,
    (SELECT NVL(MAX(SEQUENCE),0)+1 FROM XININSURE.SALEACTION WHERE SALEID = :SALEID),
    trunc(sysdate),
    :ACTIONID,
    :ACTIONSTATUS,
    :REQUESTREMARK,
    :ACTIONREMARK
)