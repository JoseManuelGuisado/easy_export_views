<?php

namespace Drupal\easy_export_views\Controller;

use Drupal\Core\Controller\ControllerBase;
use Drupal\views\Views;

use Drupal\Core\Database\Install\Tasks as InstallTasks;
use Drupal\Core\Database\Database;
use Drupal\Core\Database\Driver\mysql\Connection;
use Drupal\Core\Database\DatabaseNotFoundException;

use Drupal\Core\Language\LanguageInterface;

/**
 * An example controller.
 */
class EasyExportViews extends ControllerBase {

	/**
	 * @todo Probrar filtros de la consulta
	 *
	 * @todo Poner un nombre más apropiado al archivo de exportación
	 */



	/**
	 * Array para almacenar los elementos a exportar
	 * 
	 * @var array
	 */
	protected $array_to_export = [];

	/**
	 * fields type a excluir d ela consulta
	 * 
	 * @var array
	 */
	protected $excluded_fields_type = ['user_bulk_form', 'entity_operations'];

	/**
	 * String con todos los fields de la consulta
	 * 
	 * @var String
	 */
	protected $fields;

	/**
	 * Fields a mostrar en la exportación
	 * 
	 * @var array
	 */
	protected $fields_to_show = [];

	/**
	 * File en el que vamos a realizar la exportación
	 * 
	 * @var String
	 */
	protected $file;

	/**
	 * Path del file en el que vamos a realizar la exportación
	 * 
	 * @var String
	 */
	protected $file_path;

	/**
	 * Trozo de consulta sql para los filtros "where"
	 * 
	 * @var String
	 */
	protected $filters;

	/**
	 * Trozo de la consulta sql correspondiente al from con todas las tablas y sus correspondientes JOINS
	 * 
	 * @var String
	 */
	protected $from_text;

	/**
	 * Cabeceras del documento csv
	 * 
	 * @var array
	 */
	protected $headers = [];

	/**
	 * Consulta sql completa
	 * 
	 * @var String
	 */
	protected $q;

	/**
	 * Trozo de la consulta sql con la ordenación de los datos
	 * @var String
	 */
	protected $sort;
	
	/**
	 * Relación de todas las tablas que vamos a necesitar en la consulta
	 * 
	 * @var Array
	 */
	protected $tables = [];

	/**
	 * Vista de la que queremos extraer la consulta
	 * 
	 * @var Drupal\views\ViewExecutable
	 */
	protected $view;


	/**
	 * Función que exporta los usuarios de una vista, finalmente se hace un return para que no muestre nada después de la descarga.
	 * 
	 * @param  String $view    Nombre de la vista
	 * @param  String $display Nombre del display
	 */
	public function export($view, $display) {

		$this->load_view($view, $display);
		$this->load_headers ();
		$this->get_tables();
		$this->get_fields();
		$this->get_sort();
		$this->get_filters();
		$this->load_query();
		$this->get_csv_file();

		$query = Database::getConnection()->query($this->q);
		$result = $query->fetchAll();

		foreach ($result as $item) {
			$aux = [];

			foreach ($item as $key => $value) {
				if (in_array($key, $this->fields_to_show))
					$aux [$key] = $value;
			}
			$array_to_export[] = $aux;
			unset($aux);
		}

		self::array_to_csv($array_to_export, $this->file);
		fclose($this->file);

		//cabeceras para descarga
		header('Content-Type: application/octet-stream');
		header("Content-Transfer-Encoding: Binary"); 
		header("Content-disposition: attachment; filename=\"easy_export_views_" . uniqid() . ".csv\""); 

		readfile($this->file_path);

		unlink($this->file_path);
		exit;
	}


	/**
	 * Cargamos la vista y el display de donde necesitamos extraer los datos para generar la consulta.
	 * 
	 * @param  String $view    Nombre de la vista
	 * @param  String $display Nombre del display
	 */
	protected function load_view ($view, $display) {

		$this->view = Views::getView($view);
		$this->view->setDisplay($display);
		$this->view->execute();
	}


	/**
	 * Cargamos la cabecera del archivo csv en un array para añadirlos al carchivo cuando lo vayamos a generar.
	 * Tenemos que tener en cuenta que el field no sea 
	 */
	protected function load_headers () {
		
		foreach ($this->view->field as $id => $handler) {
			if (!in_array($handler->getPluginId(), ['user_bulk_form', 'entity_operations']) )
				$this->headers[$id] = $id;
		}
	}


	/**
	 * Construimos la query con los fields, tables, filters y order
	 */
	protected function load_query () {

		$this->q = sprintf('SELECT %s FROM %s', $this->fields, $this->from_text);
		if ($this->filters) $this->q .= ' WHERE ' . $this->filters;
		if ($this->sort)  $this->q .= ' ORDER BY ' . $this->sort;
	}


	/**
	 * Función que extrae los fields que vamos a necesitar de la propiedad $field de la vista y los devuelve como string para ponerlo directamente en la consulta
	 */
	protected function get_fields() {

		$sqlfields = [];

		foreach ($this->view->field as $key => $value) {
			if (!in_array($value->getPluginId(), ['user_bulk_form', 'entity_operations']) ) {
				if ($value->options['type'] == 'timestamp') {
					$sqlfields [$value->field] = sprintf('from_unixtime(%s.%s) as %s', $this->get_alias($value->table), $value->realField, $value->field);
				} else {
					$sqlfields [$value->field] = sprintf('%s.%s as %s', $this->get_alias($value->table), $value->realField, $value->field);
				}
				$this->fields_to_show[] = $value->field;					
			}
		}


		foreach ($this->view->query->fields as $key => $value) {
			$sqlfields [$key] = sprintf('%s.%s as %s', $value['table'], $value['field'], $value['alias']);
		}

		$this->fields = implode(', ', $sqlfields);
	}


	/**
	 * Función que analiza la propiedad field de la vista y extra las tablas que se van a necesitar para la consulta, devolviendo un string con los correspondientes joins para hacer la consulta.
	 */
	protected function get_tables() {

		$tables = [];
		$added  = []; // Necesito un array para conocer las tablas que ya se han añadadio y no repetir
		$main_table = '';

		foreach ($this->view->query->tables as $query_tables) {

			foreach ($query_tables as $table) {
				if ( $table['alias']) {
					$aux_table = $this->view->query->getTableInfo($table['alias']);
					if ($aux_table['table'] && ! in_array($aux_table['table'], $added)) {
						$main_table = $aux_table['table'];
						$added [] = $aux_table['table'];
						$tables [$aux_table['table']] = Array();
						$tables [$aux_table['table']]['table'] = $aux_table['table'];
						$tables [$aux_table['table']]['primary_key'] = $this->get_primary_key_field($aux_table['table']);
						$tables [$aux_table['table']]['alias'] = $aux_table['alias'];
						if ($aux_table['join']->type) {
							$tables [$aux_table['table']]['join type'] = $aux_table['join']->type;
							$tables [$aux_table['table']]['condition'] = sprintf('%s.%s = %s.%s', $aux_table['join']->leftTable, $aux_table['join']->leftField, $aux_table['alias'], $aux_table['join']->field);
							if ($aux_table['join']->extra) {
								foreach ($aux_table['join']->extra as $key => $value) {
									if (isset($value['value'])) {
										$tables [$aux_table['table']]['condition'] .= sprintf(" AND %s.%s = '%s'", $aux_table['alias'], $value['field'], $value['value']);
									}
								}
							}							
						}						
					}
				}
			}
		}

		// tablas para los fields
		foreach ($this->view->field as $key => $value) {

			if ( !in_array($value->getPluginId(), ['user_bulk_form', 'entity_operations']) && ! in_array($value->table, $added) ) {
				//$tables [$value->table] = sprintf('LEFT JOIN %s ON users.uid = %s.entity_id', $value->table, $value->table);
				$added [] = $value->table;
				$tables [$value->table] = Array();
				$tables [$value->table]['join type'] = 'LEFT';
				$tables [$value->table]['table'] = $value->table;
				$tables [$value->table]['primary_key'] = $this->get_primary_key_field($value->table);
				$tables [$value->table]['alias'] = $value->tableAlias ? $value->tableAlias : $value->table;
				$tables [$value->table]['condition'] = sprintf('%s.%s = %s.entity_id', $value->relationship ? $value->relationship : $tables[$main_table]['alias'], $this->get_primary_key_field($main_table), $tables [$value->table]['alias']);
			}
		}

		// tablas para los filtros
		foreach ($this->view->filter as $key => $value) {
			if ( $value->table && ! in_array($value->table, $added) ) {
				//$tables [$value->table] = sprintf('LEFT JOIN %s ON users.uid = %s.entity_id', $value->table, $value->table);
				$added [] = $value->table;
				$tables [$value->table] = Array();
				$tables [$value->table]['join type'] = 'LEFT';
				$tables [$value->table]['table'] = $value->table;
				$tables [$value->table]['alias'] = $value->tableAlias ? $value->tableAlias : $value->table;
				$tables [$value->table]['condition'] = sprintf('%s.%s = %s.entity_id', $tables[$main_table]['alias'], $this->get_primary_key_field($main_table), $tables [$value->table]['alias']);
			}
		}

		$this->tables = $tables;

		foreach ($tables as $key => $value) {
			if ($value['join type']) {
				$this->from_text .= sprintf(' %s JOIN %s %s ON %s', $value['join type'], $value['table'], $value['alias'], $value['condition']);
			} else {
				$this->from_text .= $value['table'];
			}
		}

		$aux = [];

		foreach ($tables as $table) {
			$aux [] = sprintf('%s JOIN %s ON %s', $table['join type'], $table['table'], $table['condition']);
		}

	}


	/**
	 * Función que analiza la propiedad query->orderby de la vista y extrae un string para poner en la consulta en el order by
	 */
	protected function get_sort() {

		$orderby = [];

		foreach ($this->view->query->orderby as $key => $value) {
			$orderby [] = sprintf('%s %s', $value['field'], $value['direction']);
		}
		$this->sort = implode(', ', $orderby);
	}


	/**
	 * Función que analiza la propiedad query->where de la vista y construye una condición para poner en el where de la consulta sql
	 */
	public function get_filters() {

		$where = [];
		$return = '';

		$aux = array_reverse($this->view->query->where);


		foreach ($aux as $key => $value) {
			foreach ($value['conditions'] as $condition) {
				$where[$key][] = self::build_condition ($condition ['operator'], $condition['field'], $condition['value']);
			}
	
			if (isset($where[$key - 1])) {
				$return .= ' AND ' . implode(' ' . $value['type'] . ' ', $where[$key]);
			} else {
				$return .= implode(' ' . $value['type'] . ' ', $where[$key]);
			}
		}

		$this->filters = $return;
	}


	/**
	 * Función auxiliar que construye una condición diferenciando si es un array con varios datos, el tipo de operador, etc...
	 * 
	 * @param  String $operator El operador de la condición ('<', '>', '=', 'LIKE', 'formula', 'in', ...)
	 * @param  String $field    Nombre de field para la condición
	 * @param  String $value    Valor de la condición
	 * @return String 			Toda las condiciones separadas por el operador AND
	 */
	public static function build_condition ($operator, $field, $value) {

		$aux = [];

		if ((is_array($value) && count($value)) && $operator != 'in') {
			foreach ($value as $key => $item) {
				if (preg_match('/:(\w+)/', $key)) {
					$aux [] = str_replace($key, self::implode_str(',', $item), $field);
				}
			}
		} else {
			if ( $operator != 'formula' && $operator != 'in') {
				$aux[] = sprintf("%s %s '%s'", $field, $operator, $value);
			} else if ($operator == 'in') {
				$params = [];
				foreach ($value as $param) {
					if ($param == '***LANGUAGE_language_interface***') {
						$params [] = "'" . \Drupal::languageManager()->getCurrentLanguage()->getId() . "'";
					} else {
						$params [] = "'" . $param . "'";
					}

				}
				$aux [] = sprintf('%s in (%s)', $field, implode(', ', $params));
			} else {
				$aux [] = $field;
			}
		}

		if (count($aux)) return implode(' AND ', $aux);
		return '';
	}


	/**
	 * Función auxiliar que devuelve un array o un string en una cadena de texto y con los términos entre comillas simples
	 * 
	 * @param  String 		 $glue   	La unión
	 * @param  String|array  $pieces 	Array o String con las piezas
	 * @return String         			Resultado del implode
	 */
	public static function implode_str ($glue, $pieces) {
		$aux = [];
		if (is_array($pieces)) {
			foreach ($pieces as $value) {
				$aux [] = "'$value'";
			}
		} elseif(is_string($pieces)) {
			return "'$pieces'";
		}
		return implode($glue, $aux);
	}


	/**
	 * Función que almacena los datos de un array en un archivo csv
	 * 
	 * @param  Array 	$sources 	Datos a exportar en el csv. Cada item del array debe ser un array con los datos de una fila
	 * @param  file 	$file     Archivo previamente abierto en el que guardaremos los datos
	 */
	public static function array_to_csv ($sources, $file) {
		foreach ($sources as $fields) {
			fputcsv($file, $fields, ';', '"');
		}
	}


	protected function get_alias ($table_name) {
		return $this->tables[$table_name]['alias'];
	}


	/**
	 * Genera un archivo para la exportación de los datos. Si existen las cabeceras, el archivo las añadirá al principio del todo
	 */
	protected function get_csv_file () {

		$this->file_path = '/tmp/easy_export_views_' . uniqid() . '.csv';
		clearstatcache();
		ignore_user_abort(true);     ## prevent refresh from aborting file operations and hosing file

		if (file_exists($this->file_path)) {
		   $fh = fopen($this->file_path, 'r+');
			while(1) {
			  if (flock($fh, LOCK_EX)) {
				 $buffer = chop(fread($fh, filesize($this->file_path)));
				 $buffer++;
				 rewind($fh);
				 fwrite($fh, $buffer);
				 fflush($fh);
				 ftruncate($fh, ftell($fh));     
				 flock($fh, LOCK_UN);
				 break;
			  }
		   }
		}
		else {
		   $fh = fopen($this->file_path, 'w+');
		}


		if (count($this->headers))	{
			fputcsv($fh, $this->headers, ';', '"');
		}
		$this->file = $fh;

		//	fclose($fh);
	}


	protected function get_primary_key_field ($table) {

		$query = "SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
				INNER JOIN
				    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
				          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
				             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND 
				             KU.table_name='" . $table . "' ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION LIMIT 1";

		$query = Database::getConnection()->query($query);
		$result = $query->fetchAll();

		return $result[0]->PRIMARYKEYCOLUMN;
	}

}
