from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

# Route for homepage
@app.route("/")
def index():
    return render_template("index.html")

# Route to serve charts from visualizations/output
@app.route("/charts/<filename>")
def charts(filename):
    charts_dir = os.path.join(os.path.dirname(__file__), "..", "viz", "output")
    return send_from_directory(charts_dir, filename)

if __name__ == "__main__":
    app.run(debug=True)
