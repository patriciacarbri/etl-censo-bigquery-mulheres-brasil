-- agregação segura por município + cor_raca, pivot por sexo (evita duplicação)
WITH pop_agg AS (
  SELECT
    id_municipio,
    sexo,
    grupo_idade,
    SUM(CAST(populacao AS INT64)) AS populacao_group
  FROM `basedosdados.br_ibge_censo_2022.populacao_idade_sexo`
  GROUP BY id_municipio, sexo, grupo_idade
),

a_keys AS (
  -- combinações únicas que interessam (sem repetir por 'alfabetizacao' etc.)
  SELECT DISTINCT id_municipio, cor_raca, sexo, grupo_idade
  FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca`
),

joined AS (
  SELECT
    ak.id_municipio,
    ak.cor_raca,
    ak.sexo AS sexo_a,
    COALESCE(p.populacao_group, 0) AS populacao_group
  FROM a_keys ak
  LEFT JOIN pop_agg p
    ON ak.id_municipio = p.id_municipio
    AND ak.sexo = p.sexo
    AND ak.grupo_idade = p.grupo_idade
)

SELECT
  j.id_municipio,
  m.nome AS nome_municipio,
  m.sigla_uf,
  j.cor_raca,
  SUM(CASE WHEN j.sexo_a = 'Homens'   THEN j.populacao_group ELSE 0 END)   AS populacao_homens,
  SUM(CASE WHEN j.sexo_a = 'Mulheres' THEN j.populacao_group ELSE 0 END)   AS populacao_mulheres,
  SUM(j.populacao_group) AS populacao_total
FROM joined j
LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m
  ON j.id_municipio = m.id_municipio
GROUP BY
  j.id_municipio, m.nome, m.sigla_uf, j.cor_raca
ORDER BY
  j.id_municipio, j.cor_raca;



-- WITH pop_agg AS (
--   SELECT
--     id_municipio,
--     sexo,
--     SUM(CAST(populacao AS INT64)) AS populacao_total
--   FROM `basedosdados.br_ibge_censo_2022.populacao_idade_sexo`
--   GROUP BY id_municipio, sexo
-- )

-- SELECT
--   a.id_municipio,
--   m.nome AS nome_municipio,
--   m.sigla_uf,
--   SUM(CASE WHEN p.sexo = 'Homens'   THEN p.populacao_total ELSE 0 END) AS populacao_homens,
--   SUM(CASE WHEN p.sexo = 'Mulheres' THEN p.populacao_total ELSE 0 END) AS populacao_mulheres
-- FROM (
--   -- pegamos a lista de municípios de interesse a partir da tabela 'a'
--   SELECT DISTINCT id_municipio FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca`
-- ) a
-- LEFT JOIN pop_agg p ON a.id_municipio = p.id_municipio
-- LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m
--   ON a.id_municipio = m.id_municipio
-- GROUP BY a.id_municipio, m.nome, m.sigla_uf
-- ORDER BY a.id_municipio;




-- SELECT
--   a.id_municipio,
--   m.nome AS nome_municipio,
--   m.sigla_uf,
--   a.cor_raca,
--   a.sexo,
--   a.grupo_idade,
--   a.alfabetizacao,
--   CAST(p.populacao AS INT64) AS populacao_total_grupo

-- FROM `basedosdados.br_ibge_censo_2022.alfabetizacao_grupo_idade_sexo_raca` a

-- LEFT JOIN `basedosdados.br_ibge_censo_2022.populacao_idade_sexo` p
--   ON a.id_municipio = p.id_municipio

-- LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` m
--   ON a.id_municipio = m.id_municipio
-- ;
