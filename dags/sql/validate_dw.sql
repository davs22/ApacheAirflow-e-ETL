-- Falha a tarefa em caso de perda de linhas, valores divergentes ou ausência de dados.
DO $$
DECLARE origem BIGINT; destino BIGINT; receita_origem NUMERIC; receita_dw NUMERIC;
BEGIN
 SELECT COUNT(*),SUM(linetotal) INTO origem,receita_origem FROM stg.salesorderdetail;
 SELECT COUNT(*),SUM(valor_total) INTO destino,receita_dw FROM dw_f_vendas;
 IF origem=0 THEN RAISE EXCEPTION 'Origem vazia: bootstrap não comprovado'; END IF;
 IF origem<>destino THEN RAISE EXCEPTION 'Contagem divergente: staging %, DW %',origem,destino; END IF;
 IF receita_origem IS DISTINCT FROM receita_dw THEN
   RAISE EXCEPTION 'Receita divergente: staging %, DW %',receita_origem,receita_dw;
 END IF;
 IF EXISTS(SELECT 1 FROM stg.salesorderdetail s LEFT JOIN dw_f_vendas f
   ON f.sales_order_detail_id=s.salesorderdetailid WHERE f.sales_order_detail_id IS NULL
   OR ROW(s.orderqty,s.unitprice,s.unitpricediscount,s.linetotal) IS DISTINCT FROM
      ROW(f.quantidade,f.preco_unitario,f.desconto,f.valor_total)) THEN
   RAISE EXCEPTION 'Itens ausentes ou medidas divergentes';
 END IF;
 IF EXISTS(SELECT 1 FROM dw_f_vendas WHERE quantidade<=0 OR desconto<0 OR desconto>1
    OR ABS(valor_total - quantidade*preco_unitario*(1-desconto))>0.000001) THEN
   RAISE EXCEPTION 'Medidas inválidas ou cálculo de receita inconsistente';
 END IF;
END $$;
SELECT 'Qualidade OK' resultado, COUNT(*) itens, SUM(valor_total) receita FROM dw_f_vendas;
