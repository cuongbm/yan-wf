import pytest
import sys

sys.path.append("C:\\_works\\yan-wf\\src")
print(sys.path)
@pytest.fixture(scope="session", autouse=True)
def do_something(request):
    print("xxxxxxxxxpath")

