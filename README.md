# Clinic Reviews System

A comprehensive system for scraping, analyzing, and scoring healthcare clinic reviews from Google Maps and Trustpilot.

## 🏗️ Architecture

The system follows a streamlined 4-stage pipeline:

1. **Scrape** - Extract reviews from Google Maps and Trustpilot
2. **Unify** - Combine raw reviews into standardized format (multilingual)
3. **Enrich** - Analyze reviews using AI to extract sentiment attributes
4. **Score** - Calculate clinic scores based on enriched data

## 📁 Project Structure

```
clinic-reviews-system/
├── config/                 # Configuration files
│   ├── scraping_config.yaml
│   ├── enrichment_config.yaml
│   └── scoring_config.yaml
├── tokens/                 # API tokens (not in repo)
│   ├── mongodb_connection.txt
│   ├── apify_token.txt
│   └── google_api_key.txt
├── src/                    # Source code
│   ├── core/              # Core functionality
│   ├── scrapers/          # Data scraping
│   ├── processors/        # Data processing
│   └── utils/             # Utilities
├── data/                  # Data files
│   └── establishments.xlsx
├── logs/                  # Log files
├── main.py               # CLI entry point
└── requirements.txt      # Dependencies
```

## 🚀 Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd clinic-reviews-system
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Setup tokens**
Create the following files in the `tokens/` directory:
- `mongodb_connection.txt` - MongoDB Atlas connection string
- `apify_token.txt` - Apify API token
- `google_api_key.txt` - Google Gemini API key

4. **Setup data**
Place your establishments Excel file in `data/establishments.xlsx` with columns:
- `displayName` - Name of the establishment
- `googleUrl` - Google Maps URL
- `website` - Website URL

## 🎯 Usage

### Basic Commands

```bash
# Show help
python main.py --help

# Scrape reviews from establishments
python main.py scrape --excel data/establishments.xlsx

# Unify raw reviews into standardized format
python main.py unify

# Enrich reviews with AI analysis
python main.py enrich

# Calculate clinic scores
python main.py score

# Show database statistics
python main.py stats
```

### Advanced Options

```bash
# Process specific establishments only
python main.py unify --establishments "id1,id2,id3"

# Enrich only specific attribute groups
python main.py enrich --attributes sentiment,complaint

# Process reviews published after a date
python main.py enrich --published-after 2024-01-01T00:00:00.000Z

# Quick mode (minimal output)
python main.py unify --quick

# Verbose logging
python main.py scrape --excel data/establishments.xlsx --verbose
```

## 📊 Data Pipeline

### 1. Scraping
- Extracts reviews from Google Maps and Trustpilot using Apify
- Stores raw data in `google_reviews` and `trustpilot_reviews` collections
- Preserves original language and all metadata

### 2. Unification
- Combines reviews into standardized `unified_reviews` collection
- Maps platform-specific fields to common schema
- Maintains data in original languages (no translation)

### 3. Enrichment
- Analyzes reviews using Google Gemini AI
- Extracts sentiment attributes (0-3 scale):
  - `staff_satisfaction`
  - `treatment_satisfaction`
  - `facility`
  - `scheduling`
  - `onsite_communication`
  - `post_op`
  - `affordability`
  - `recommendation`
  - `accommodation_transportation`
- Identifies complaints (`is_complaint`)
- Analyzes response quality for complaints
- Stores results in `enriched_reviews` collection

### 4. Scoring
- Calculates Bayesian adjusted ratings
- Computes NPS-style scores (-100 to +100) for each attribute
- Creates composite scores:
  - **Service Quality Score** (treatment, post-op, staff, facility)
  - **Communication Score** (onsite communication, scheduling, online communication)
- Updates establishment records with calculated scores

## 🔧 Configuration

### Scraping Config (`config/scraping_config.yaml`)
```yaml
google_maps:
  actor_id: "Xb8osYTtOjlsgI6k9"
  settings:
    language: "en"
    max_reviews: 99999

trustpilot:
  actor_id: "fLXimoyuhE1UQgDbM"
  settings:
    count: 99999
    languages: "all"
```

### Enrichment Config (`config/enrichment_config.yaml`)
```yaml
# Target specific establishments (empty = all)
target_establishments: []

# AI processing settings
processing:
  batch_size: 25
  sentiment_batch_size: 3
  min_review_length: 10
  max_tokens: 700000

# Sentiment attributes to analyze
sentiment_attributes:
  staff_satisfaction:
    description: "Sentiment about staff quality"
    enabled: true
```

### Scoring Config (`config/scoring_config.yaml`)
```yaml
# Bayesian prior weight
bayesian:
  prior_weight: 100

# Composite score weights
service_quality_weights:
  treatment_satisfaction: 0.30
  post_op: 0.20
  staff_satisfaction: 0.30
  facility: 0.20
```

## 🗄️ Database Collections

### Raw Collections
- `establishments` - Clinic information
- `google_reviews` - Raw Google Maps reviews
- `trustpilot_reviews` - Raw Trustpilot reviews

### Processed Collections
- `unified_reviews` - Standardized reviews (multilingual)
- `enriched_reviews` - AI-analyzed reviews with attributes

## 📈 Monitoring

### View Statistics
```bash
python main.py stats
```

### Check Processing Coverage
```bash
# Check enrichment progress
python main.py enrich --quick

# Check scoring status
python main.py score --quick
```

### Logs
All operations are logged to `logs/clinic_reviews.log`

## 🔄 Typical Workflows

### Initial Setup (New System)
```bash
# 1. Scrape all establishments
python main.py scrape --excel data/establishments.xlsx

# 2. Unify reviews
python main.py unify

# 3. Enrich with AI analysis
python main.py enrich

# 4. Calculate scores
python main.py score

# 5. Check results
python main.py stats
```

### Regular Updates (Daily/Weekly)
```bash
# 1. Re-scrape existing establishments (gets new reviews)
python main.py scrape --excel data/establishments.xlsx

# 2. Process new reviews (incremental)
python main.py unify --quick
python main.py enrich --quick
python main.py score --quick
```

### Adding New Establishments
```bash
# 1. Add to Excel file, then scrape
python main.py scrape --excel data/new_establishments.xlsx

# 2. Process new data
python main.py unify --quick
python main.py enrich --quick
python main.py score --quick
```

## 🚨 Error Handling

- All operations are incremental by default (only process new data)
- Failed API calls are logged but don't stop processing
- Database operations use batch processing to handle large datasets
- Each stage can be run independently for troubleshooting

## 💡 Key Features

- **Multilingual Support** - Processes reviews in original languages
- **Incremental Processing** - Only processes new/unprocessed data
- **Batch Processing** - Handles large datasets efficiently
- **Configurable** - Easy to adjust settings and weights
- **Comprehensive Logging** - Detailed logs for monitoring and debugging
- **CLI Interface** - Simple command-line operations
- **Modular Design** - Each component can run independently

## 🔑 API Usage & Costs

### Apify (Scraping)
- Charged per actor run
- Google Maps: ~$0.50 per 1000 reviews
- Trustpilot: ~$0.30 per 1000 reviews

### Google Gemini (AI Analysis)
- Charged per token (input + output)
- Typical usage: ~1000 tokens per review
- Cost: ~$0.001 per review analyzed

## 🐛 Troubleshooting

### Common Issues
1. **Token errors** - Check all token files exist and are valid
2. **Database connection** - Verify MongoDB connection string
3. **API limits** - Monitor API quotas and adjust batch sizes
4. **Memory issues** - Reduce batch sizes in config files

### Debug Mode
```bash
python main.py <command> --verbose
```

## 📝 License

[Your license here]

## 🤝 Contributing

[Contributing guidelines here]