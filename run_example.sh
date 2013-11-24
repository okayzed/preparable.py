for example in examples/*.py; do
  if [ 'examples/__init__.py' != $example ]; then
    echo -e "\n*** RUNNING" $example "***"
    PYTHONPATH=. python $example
  fi
done
