SELECT
  '2010' AS ano,
  uf AS sigla_uf,             -- ajuste conforme seu schema (pode ser cod_uf/nome_uf/etc)
  grupo_idade,                -- ex.: '0-4', '5-9', ...
  raca_cor,                   -- ex.: 'Branca', 'Preta', 'Parda', 'Amarela', 'Indígena'
  SUM(populacao) AS total_mulheres
FROM `{{project_id}}.{{dataset_2010}}.populacao_grupo_idade_sexo_raca`
WHERE sexo = 'Feminino'       -- ajuste se o valor for outro (ex.: 'F')
GROUP BY
  sigla_uf,
  grupo_idade,
  raca_cor
ORDER BY
  sigla_uf,
  grupo_idade,
  raca_cor;