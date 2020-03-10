cat /workspace/.devcontainer/welcome.txt
echo "Fish is your friend!!!"
echo "TYPE ""fish"""
alias stopall="docker container stop \$(docker container ls -aq)"
alias removeall="docker container rm \$(docker container ls -aq)"
 
