for test in tests/*.py; do
  if [ 'tests/__init__.py' != $test ]; then
    echo -e "\n*** RUNNING" $test "***"
    PYTHONPATH=. python3 $test
  fi
done
