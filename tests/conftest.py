# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Configuracion Entorno De Prueba"""

# Modulos Python
from pathlib import Path

# Modulos terceros
import pytest
import shutil

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    # Definimos una carpeta de tests aislada
    test_data_dir = Path.cwd() / 'data' / 'test_env'
    test_data_dir.mkdir(parents=True, exist_ok = True)
    
    yield test_data_dir # Aquí se ejecutan los tests
    
    # Opcional: Limpiar al finalizar
    # shutil.rmtree(test_data_dir)